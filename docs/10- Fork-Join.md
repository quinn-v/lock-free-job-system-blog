***

Not everything needs to be a co-routine. Regular functions and lambdas can participate in fork/join parallelism through a separate, lighter-weight path that doesn't require any co-routine machinery. For parallel iteration in particular, this path is recommended- stack-allocated tasks and a simple latch are generally faster than co-routine frames for tight divide-and-conquer loops.

***

**Non-co-routine fork/join**

The `join()` free function runs two callables in parallel using `StackTask` and `help_until`:

```cpp
template <Scheduler S, std::invocable A, std::invocable B>
auto join(ThreadPool<S> &pool, A &&func_a, B &&func_b)
    -> typename JoinTraits<A, B>::ReturnType
{
    using Traits = JoinTraits<A, B>;
    using ResA   = typename Traits::ResA;

    ResultStorage<ResA> result_a_store;
    SimpleLatch latch;

    task::StackTask b_task = task::make_stack(&latch, std::forward<B>(func_b));
    task::TaskRef b_ref = b_task.as_ref();
    pool.enqueue(b_ref);

    result_a_store.capture([&]() { return std::invoke(std::forward<A>(func_a)); });

    pool.help_until([&latch]() { return latch.probe(); });

    return Traits::Result::build(
        [&] { return result_a_store.get(); },
        [&] { return b_task.into_result(); }
    );
}
```

`func_b` is wrapped in a `StackTask`- a task type that lives on the caller's stack- with a pointer to a `SimpleLatch`. No heap allocation is required here. 
The task is enqueued on the pool, then `func_a` runs inline on the calling thread. 
Once `func_a` finishes, `help_until` keeps stealing and executing tasks from the pool until `latch.probe()` returns true- meaning `func_b` has finished and set the latch. 
This is required, as otherwise you will eventually reach a dead-lock, where all threads are waiting for a task to finish, that hasn't (and now can't), start.

If `func_b` hasn't been picked up yet, the calling thread may steal and execute it itself.

Result types collapse naturally: if either callable returns `void`, the result is dropped and the other side's return value is returned directly. If both return `void`, the function returns `void`. The `JoinTraits` template handles all four combinations at compile time.

***

**Parallel iterators**

`join()` composes recursively into a divide-and-conquer pattern for data parallelism:

```cpp
template <Scheduler S, SpanRange Range, RangeForFunction<Range> Fn>
void for_each(ThreadPool<S> &pool, Range &&range, const Fn &func, size_t grain_size)
{
    using T = std::ranges::range_value_t<Range>;
    detail::for_each_impl(pool, std::span<T>(std::forward<Range>(range)), func, grain_size);
}
```

The implementation splits the range at the midpoint and calls `join()` on the two halves recursively:

```cpp
void for_each_impl(Pool &pool, std::span<T> data, const Fn &func, size_t grain_size)
{
    if (data.size() <= grain_size || data.size() <= 1)
    {
        for (T &value : data) func(value);
        return;
    }

    const size_t mid = data.size() / 2;
    join(
        pool,
        [&]() { for_each_impl(pool, data.subspan(0, mid), func, grain_size); },
        [&]() { for_each_impl(pool, data.subspan(mid),    func, grain_size); }
    );
}
```

When the range is small enough (≤ `grain_size` elements), it falls back to a serial loop. The tree of `join()` calls naturally fills the thread pool with work while the calling thread participates.

`for_each_auto` removes the need to choose `grain_size` manually. It derives a reasonable value from the pool size and a configurable batch factor:

```cpp
template <Scheduler S, SpanRange Range, RangeForFunction<Range> Fn>
void for_each_auto(ThreadPool<S> &pool, Range &&range, const Fn &func, size_t batch_size = 4)
{
    using T = std::ranges::range_value_t<Range>;

    const size_t count = std::ranges::size(range);
    if (count == 0) return;

    const size_t thread_count = pool.worker_count();
    size_t grain_size = std::max(size_t{1}, count / (thread_count * batch_size));
    detail::for_each_impl(pool, std::span<T>(std::forward<Range>(range)), func, grain_size);
}
```

`batch_size = 4` means each worker gets roughly 4 chunks to work through. This leaves some slack for load imbalance- if one chunk takes longer than expected, other workers can pick up the remaining ones rather than sitting idle.

For index-based iteration there is `par_for`:

```cpp
par_for(start, end, func, ForConfig{ .executor = &pool, .grain = grain_size });
```

The same divide-and-conquer structure, but operating on `[start, end)` index ranges rather than spans.

***Note; Since the tasks used in the fork/join operations are fully stack allocated. If you create a very large amount of tasks, it may cause a stack overflow. During general usage however, this shouldn't occur.

***

**WaitGroup**

For cases where you want to fire a batch of tasks and wait for all of them without a structured join, `WaitGroup` provides a simple counter:

```cpp
WaitGroup wg;
wg.add(n);

for (size_t i = 0; i < n; ++i)
{
    enqueue_task(pool, [&wg, i]() -> Task<void>
    {
        do_work(i);
        wg.done();
        co_return;
    }());
}

// blocks until count reaches zero
wg.wait();
```

`done()` decrements the counter and calls `notify_all()` when it hits zero. `wait()` uses `atomic::wait()` to avoid burning CPU- it suspends the calling thread until the notification arrives.

`WaitGroup` is appropriate when the fan-out count is dynamic or when you don't need to collect results. For structured parallelism with results, prefer `join_task` (co-routines) or `join` (functors).


**Join Traits
---

Here is a snippet of the JoinResult and Traits. 
These are simple template specialization for when a function for example returns void, so both void and value returns are valid.

```cpp
template <>
struct ResultStorage<void>
{
    template <typename Func>
    void
    capture(Func &&func)
    {
        std::invoke(std::forward<Func>(func));
    }

    void
    get()
    {
    }
};

template <typename ResA, typename ResB>
struct JoinResult
{
    using Type = typename JoinResultType<ResA, ResB>::Type;

    template <typename GetA, typename GetB>
    static Type
    build(GetA &&get_a, GetB &&get_b)
    {
        if constexpr (std::is_void_v<ResA> && std::is_void_v<ResB>)
        {
            return;
        }
        else if constexpr (std::is_void_v<ResA>)
        {
            return std::invoke<GetB>(std::forward<GetB>(get_b));
        }
        else if constexpr (std::is_void_v<ResB>)
        {
            return std::invoke<GetA>(std::forward<GetA>(get_a));
        }
        else
        {
            return Type{
                std::invoke<GetA>(std::forward<GetA>(get_a)),
                std::invoke<GetB>(std::forward<GetB>(get_b)),
            };
        }
    }
};

template <typename A, typename B>
struct JoinTraits
{
    using ResA = std::invoke_result_t<A>;
    using ResB = std::invoke_result_t<B>;
    using Result = JoinResult<ResA, ResB>;
    using ReturnType = typename Result::Type;
};
```
