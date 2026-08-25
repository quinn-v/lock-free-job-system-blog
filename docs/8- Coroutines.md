***

C++20 coroutines let a function suspend itself and resume later without blocking a thread. The compiler transforms a coroutine function into a state machine; the state is stored in a heap-allocated frame. Execution can be handed off to another thread simply by resuming the frame from there.

This is a great fit for a work-stealing scheduler. A coroutine that needs to await another task can suspend itself, enqueue the awaited task, and let the scheduler resume it when the result is ready- without blocking a worker thread.

This section assumes basic familiarity with the coroutine machinery: `co_await`, `co_return`, the `promise_type` protocol, and `std::coroutine_handle<>`. The focus here is on how to wire coroutines into the job system.

***

**The Task type**

The coroutine task type is `Task<T>`. It wraps a `std::coroutine_handle<TaskPromise<T>>` and is `[[nodiscard]]`- discarding a `Task` without awaiting or enqueue-ing it leaks the compiler allocated co-routine frame.

```cpp
template <typename T>
class Task final
{
  public:
    using promise_type = TaskPromise<T>;
    // ...
};
```

The promise's `initial_suspend` returns `std::suspend_always`, so the co-routine body doesn't start until something explicitly resumes or enqueues it. This lets you construct a `Task` and decide where to run it before any work begins.

```cpp
Task<int> compute()
{
    co_return 42;
}

// The coroutine is created but not yet started.
Task<int> t = compute();
```

***

**Custom frame allocation**

This is where the per-worker allocator from the previous section comes into a play. 
C++20 allows a `promise_type` to override `operator new` and `operator delete`, giving you control over where the co-routine frame lives.

The challenge: when the frame is deleted, we need to know which allocator it came from, but `operator delete` only receives the pointer and size. A solution is to over-allocate the frame by a pointer-sized header, store the allocator address there, and hand the compiler a pointer offset by that header. 
Do note, as a hardened version, you may want to insert guard pages here.

```cpp
static constexpr size_t alignment   = alignof(std::max_align_t);
static constexpr size_t header_size =
    (sizeof(IAllocator *) + alignment - 1) & ~(alignment - 1);

void *operator new(size_t size)
{
    const size_t total = size + header_size;

    if (auto *current = IWorker::current())
    {
        auto *alloc = current->allocator();
        void *ptr = alloc->allocate(total, alignment);

        if (ptr != nullptr)
        {
            *reinterpret_cast<IAllocator **>(ptr) = alloc;
            return static_cast<char *>(ptr) + header_size;
        }
    }

    void *ptr = ::operator new(total);
    *reinterpret_cast<IAllocator **>(ptr) = nullptr;
    return static_cast<char *>(ptr) + header_size;
}
```

On `new`: if a worker is running on the current thread, use its per-worker allocator and store a pointer to it in the header. Fall back to `::operator new` (storing `nullptr` as the allocator) if called from a non-worker context.

```cpp
void operator delete(void *ptr, size_t size) noexcept
{
    const size_t total = size + header_size;
    void *real   = static_cast<char *>(ptr) - header_size;
    IAllocator *alloc = *reinterpret_cast<IAllocator **>(real);

    if (alloc == nullptr)
    {
        ::operator delete(real);
        return;
    }

    if (auto *current = IWorker::current())
    {
        if (current->allocator() == alloc)
        {
            alloc->deallocate(real, total, alignment);
            return;
        }
    }

    alloc->ts_deallocate(real, total, alignment);
}
```

On `delete`: recover the original pointer by subtracting the header, read the stored allocator. If the deleting thread owns the allocator, use the fast non-thread-safe path. If it's a different thread (the frame was stolen), use `ts_deallocate`- the SPMC allocator's CAS path.

***

**Continuations and symmetric transfer**

When a coroutine completes, it needs to resume whoever was waiting on it. We store this continuation as a `std::coroutine_handle<>` in the promise, set before the child starts running:

```cpp
void set_continuation(std::coroutine_handle<> continuation) noexcept
{
    continuation_ = continuation;
}
```

The promise also holds a few other fields that the `FinalAwaiter` reads to decide what to do on completion:

```cpp
private:
    std::optional<T>        result_{};               // non-void T only
    std::coroutine_handle<> continuation_{};
    IExecutor              *continuation_executor_{};
    void (*on_complete_)(void *){};
    void *on_complete_data_{};
```

`on_complete_` and `on_complete_data_` are used by parallel joins: the `JoinAwaitable` sets a callback that decrements a counter, so that the last-finishing task resumes the caller. `continuation_executor_` is the target executor when `resume_on` has been called on the task.

At final suspend, a `FinalAwaiter` decides what to resume. Returning a `coroutine_handle<>` from `await_suspend` is a *symmetric transfer*- the compiler turns it into a tail-call, so long chains don't grow the stack.

```cpp
struct FinalAwaiter
{
    bool await_ready() const noexcept { return false; }

    std::coroutine_handle<>
    await_suspend(std::coroutine_handle<TaskPromise<T>> handle) const noexcept
    {
        auto &promise = handle.promise();

        // Join completion: notify the join counter.
        if (promise.on_complete_)
        {
            promise.on_complete_(promise.on_complete_data_);
            return std::noop_coroutine();
        }

        if (promise.continuation_)
        {
            // Cross-executor: re-enqueue the continuation instead of resuming inline.
            if (promise.continuation_executor_)
            {
                enqueue_handle(promise.continuation_executor_, promise.continuation_);
                return std::noop_coroutine();
            }

            // Symmetric transfer- resume the parent directly.
            return promise.continuation_;
        }

        // Fire-and-forget: no owner, self-destroy.
        handle.destroy();
        return std::noop_coroutine();
    }

    void await_resume() const noexcept {}
};
```

The three meaningful paths:
- **Join counter**: `on_complete_` is set; the callback decrements an atomic counter, and when it hits zero the awaiting co-routine is resumed (covered below). The join manages frame lifetime, so we return `noop_coroutine` here.
- **Cross-executor**: a continuation exists and `continuation_executor_` is set; re-enqueue the continuation as a plain `TaskRef` on that executor and return `noop_coroutine`.
- **Symmetric transfer**: a continuation exists with no executor override; return the parent handle directly. The compiler tail-calls into it with zero stack growth regardless of chain depth.

***

**Awaiting a task**

`Task<T>` exposes `operator co_await()` which returns a `TaskAwaitable<T>`. This is what makes `co_await some_task` work:

```cpp
class TaskAwaitable final
{
  public:
    bool await_ready() const noexcept { return false; }

    std::coroutine_handle<>
    await_suspend(std::coroutine_handle<> caller) const noexcept
    {
        handle_.promise().set_continuation(caller);
        return handle_;  // symmetric transfer into the child
    }

    T await_resume() noexcept
    {
        return handle_.promise().result();
    }
};
```

`await_suspend` records the caller as the child's continuation, then returns the child's handle- a symmetric transfer that starts the child. When the child reaches `FinalAwaiter`, it transfers back to the caller. The caller then calls `await_resume()` to extract the result.

```cpp
Task<int> producer() { co_return 42; }

Task<void> consumer()
{
    int result = co_await producer();  // suspends, runs producer, resumes here
    assert(result == 42);
}
```

***

**Scheduling a task (fire-and-forget)**

To submit a coroutine to the thread pool without awaiting it, convert it to a `TaskRef` and enqueue it:

```cpp
template <typename T>
void enqueue_task(IExecutor &executor, Task<T> task)
{
    auto ref = task.release_ref();
    executor.enqueue(ref);
}
```

`release_ref()` extracts the handle address and the promise's static `execute` method into a plain `TaskRef` and transfers ownership. When the worker calls `ref.function(ref.ptr)`, it resumes the coroutine. The frame will be destroyed by `FinalAwaiter` when the coroutine completes, since there is no continuation set.

```cpp
Task<void> work() { /* ... */ co_return; }

enqueue_task(pool, work());  // fire and forget
```

***

**Coroutine handles and TaskRef**

A `std::coroutine_handle<>` is, under the hood, a plain pointer to the coroutine frame. This maps exactly to our `TaskRef`:

```cpp
// What release_ref() does internally:
TaskRef ref{
    .ptr      = handle.address(),          // frame pointer as void*
    .function = &promise_type::execute,    // static resume trampoline
};
```

`handle.address()` returns the raw frame pointer. `promise_type::execute` reconstructs the typed handle from that pointer and calls `.resume()`. The indirection cost is one function call- identical to any other `TaskRef`.

```cpp
static void execute(void *ptr)
{
    std::coroutine_handle<TaskPromise<T>>::from_address(ptr).resume();
}
```

This is why co-routines fit very well with the chosen Task type. 

Note that while regular task types use `TaskRef::create(this)` to auto-generate the trampoline via `operator()`, co-routine tasks construct their `TaskRef` directly. 
The compiler manages the frame and provides the address; there is no object to call `operator()` on.

***

**Running tasks in parallel**

Awaiting one task at a time is sequential. To run two tasks in parallel, use `join_task`:

```cpp
auto [a, b] = co_await join_task(executor, task_a(), task_b());
```

`JoinAwaitable<A, B>` manages this. In `await_suspend`:

1. An atomic counter is initialized to 2.
2. An `on_complete` callback is set on both tasks- it decrements the counter and resumes the caller when it hits zero.
3. Task B is enqueued on the executor (a worker picks it up).
4. Task A is symmetric-transferred into directly (starts on the current worker).

```cpp
std::coroutine_handle<>
await_suspend(std::coroutine_handle<> caller) noexcept
{
    caller_ = caller;

    auto on_complete = [](void *ptr)
    {
        auto *self = static_cast<JoinAwaitable *>(ptr);
        if (self->remaining_.fetch_sub(1, std::memory_order_acq_rel) == 1)
        {
            self->caller_.resume();
        }
    };

    handle_a_.promise().set_on_complete(on_complete, this);
    handle_b_.promise().set_on_complete(on_complete, this);

    TaskRef ref_b{ .ptr = handle_b_.address(), .function = &TaskPromise<B>::execute };
    executor_->enqueue(ref_b);

    return handle_a_;  // run A inline
}
```

Whichever task finishes second calls `caller_.resume()`. The acquire-release on the fetch_sub forms a happens-before edge so both results are visible when the caller reads them.

`await_resume()` then collects the results. When either type is `void`, the result is simply dropped:

```cpp
result_type await_resume() noexcept
{
    if constexpr (std::is_void_v<A> && std::is_void_v<B>) return;
    else if constexpr (std::is_void_v<A>) return handle_b_.promise().result();
    else if constexpr (std::is_void_v<B>) return handle_a_.promise().result();
    else return { handle_a_.promise().result(), handle_b_.promise().result() };
}
```

For three or more tasks, `join_task` has an overload accepting a variadic pack. It uses the same pattern- set `on_complete` on all tasks, enqueue `[0..N-2]`, and symmetric-transfer into task `N-1`.

***

**Clang note**

Clang supports `[[clang::coro_await_elidable]]` on a co-routine type. When the compiler can prove that a `Task` is immediately awaited, it can elide the heap allocation entirely by stack-allocating the frame. This is already applied to `Task<T>` in the library. It has no effect on other compilers and is harmless to include behind a pre-processor guard:

```cpp
#ifdef __clang__
#define CORO_AWAIT_ELIDABLE [[clang::coro_await_elidable]]
#else
#define CORO_AWAIT_ELIDABLE
#endif

template <typename T>
class CORO_AWAIT_ELIDABLE Task final { /* ... */ };
```
