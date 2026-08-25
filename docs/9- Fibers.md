***

C++20 coroutines are stackless- a coroutine can only suspend at an explicit `co_await` point, and it cannot suspend from inside a nested regular function call. If you need a function that can suspend itself mid-body without an explicit `co_await` at every level, you want a fiber: a stackful coroutine with its own dedicated call stack. Do note; fibers are usually quite a bit more expensive than co-routines.

The task type we built makes adding fiber support straightforward. A fiber is ultimately just state that can be resumed- which maps directly to a `void*` and a resume function.

***

**Boost.Context**

Boost.Context provides a portable, low-level context-switching primitive. Its `fiber` type represents an execution context with its own stack that can be suspended and resumed cooperatively.
Creating your own version requires writing custom assembly to save and load the relevant registers for different platforms, hence Boost.Context is used here.

Add the dependency:

```lua
add_requires("boost", { configs = { context = true } })
add_packages("boost")
```

***

**Mapping a fiber to a task**

A `boost::context::fiber` is created with a callable and executes it on a dedicated stack. When the fiber calls `.resume()` on the sink it receives, it suspends and control returns to the caller. This maps cleanly to our `Task` model:

```cpp
#include <boost/context/fiber.hpp>

namespace ctx = boost::context;

class FiberTask
{
    ctx::fiber _fiber;

  public:
    template <typename Fn>
    explicit FiberTask(Fn &&fn) :
        _fiber(std::forward<Fn>(fn))
    {
    }

    [[nodiscard]] bool
    done() const noexcept
    {
        return !_fiber;
    }

    void
    operator()()
    {
        _fiber = std::move(_fiber).resume();
    }
};
```

`operator()` resumes the fiber. The fiber runs until it suspends (by calling `.resume()` on its sink) or completes. If it suspended, `_fiber` is still valid and the task can be re-enqueued. If it completed, `_fiber` is empty.

`TaskRef::create(this)` generates the trampoline automatically since `FiberTask` is invocable via `operator()`. No additional glue is needed.

***

**Usage**

```cpp
auto *fiber_task = alloc->create<FiberTask>(
    [](ctx::fiber &&sink)
    {
        std::println("Fiber: step 1");
        sink = std::move(sink).resume();  // yield

        std::println("Fiber: step 2");
        // return implicitly completes the fiber
        return std::move(sink);
    }
);

pool.enqueue(fiber_task->as_ref());
```

When the worker executes the task, the fiber runs to the first `sink.resume()` call, suspends, and control returns through `FiberTask::operator()`. The worker can then continue executing other tasks. The fiber can be re-enqueued at any time and will continue from where it yielded.

***

**Re-enqueueing on yield**

For the fiber to automatically re-enqueue itself after yielding, it needs access to the executor. The simplest approach is to pass the executor into the fiber body and use the worker's allocator context:

```cpp
auto *fiber_task = alloc->create<FiberTask>(
    [&pool](ctx::fiber &&sink)
    {
        do_work_phase_1();

        // yield: enqueue self and suspend
        // the calling operator() will see _fiber is still valid
        sink = std::move(sink).resume();

        do_work_phase_2();
        return std::move(sink);
    }
);

task::TaskRef ref = fiber_task->as_ref();
pool.enqueue(ref);
```

After `FiberTask::operator()` returns, the caller (the worker loop) checks whether to re-enqueue. A wrapper that does this automatically:

```cpp
class AutoFiberTask
{
    ctx::fiber _fiber;
    IExecutor *_executor;

  public:
    template <typename Fn>
    AutoFiberTask(IExecutor *executor, Fn &&fn) :
        _fiber(std::forward<Fn>(fn)),
        _executor(executor)
    {
    }

    [[nodiscard]] IAllocator *
    get_allocator() const noexcept
    {
        return _executor->allocator();
    }

    void
    operator()()
    {
        _fiber = std::move(_fiber).resume();

        if (_fiber)
        {
            _executor->enqueue(TaskRef::create(this));
        }
        else
        {
            // Fiber completed- reclaim the task memory.
            AllocatorDestroyer{}(this);
        }
    }
};
```

This is the key point of the additive design: fiber support required no changes to the scheduler, the worker loop, or the allocator. The `Task` abstraction is flexible enough to represent any unit of resumable work, and the `Destroyer` pattern handles any type of cleanup.
