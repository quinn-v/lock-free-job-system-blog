***

**Results**

At this point we have a complete lock-free job system: a 16-byte type-erased task, a work-stealing scheduler with per-worker SPMC slab allocation, stackless co-routine support with custom frame allocation, stackful fiber support, fork-join parallelism, and cancellation- all without a single mutex on the hot path.

The benchmarks below compare this implementation against a naive global deque with a mutex lock, and against libfork.

``Results on an ryzen 9 5900x, on CachyOs, kernel 7.0.10-2-cachyos, averaged over multiple runs;

	- libfork: 2,238,059ns per iter
	- ytl (this): 2,812,102ns per iter
	- fib-async: 7,825,830ns per iter

***

``ytl:
```cpp
ytl::task::Task<size_t>
fib_ytl(size_t n)
{
    if (n < 2)
    {
        co_return n;
    }

    const auto [a, b] = co_await ytl::task::join_task(
        *ytl::get_executor(),
        fib_ytl(n - 1),
        fib_ytl(n - 2)
    );

    co_return a + b;
}
```

``libfork:
```cpp

inline constexpr auto fib = [](auto fib, size_t n) -> lf::task<size_t>
{
    if (n < 2)
    {
        co_return n;
    }

    size_t a;
    size_t b;

    co_await lf::fork[&a, fib](n - 1);
    co_await lf::call[&b, fib](n - 2);

    co_await lf::join;

    co_return a + b;
};

```
``Fib Async:
```cpp
size_t
fib_async(size_t n)
{
    if (n < 2)
    {
        return n;
    }

    std::future<size_t> future_a = std::async(std::launch::async, fib, n - 1);

    size_t b = fib(n - 2);
    size_t a = future_a.get();

    return a + b;
}
```

**Where to go from here**

Some directions that would extend the system further:

- **Batched stealing**- steal multiple tasks per attempt rather than one. Reduces steal overhead when the victim has many tasks queued.
- **Priority queues**- a separate high-priority local queue that always pops before the main queue. Useful for latency-sensitive tasks like input handling.
- **NUMA awareness**- prefer stealing from workers on the same NUMA node before crossing a memory domain. Requires per-node worker groups and a two-level steal policy. Can be added by creating a new Scheduler Strategy, or extending the current one.
- **Adaptive grain sizes**- measure wall time during recursion and adjust grain sizes dynamically, similar to what Rayon for example does. Reduces over-decomposition on heterogeneous work.

The 64-worker limit in `AtomicWaker` can be lifted by either switching to a wider integer or using multiple wakers -one per 64-worker group- and composing their `wake()` calls.
