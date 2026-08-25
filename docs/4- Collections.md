***

With the Task abstraction in place, let's get started on the required collections.

The three collections that make up the scheduling backbone are:

- **Global injection queue** (MPMC)- any thread can submit tasks here. Producers and consumers can race freely.
- **Worker-local queue** (SPMC Chase-Lev deque)- each worker owns one. The owner pushes and pops from one end (LIFO); thieves steal from the other end (FIFO).
- **Waker / notifier**- tracks which workers are sleeping and wakes them when new work arrives. Covered in the next section.

The data flow is: external tasks enter via the MPMC queue; each worker drains its own local deque first, then checks the global queue, then attempts to steal from another worker's deque. Keeping the hot path local avoids contention on a shared structure in the common case.

I am using a bounded Chase-Lev Deque style implementation for the worker-local queue. 
It uses a circular fixed-size buffer, with atomics for the head/tail indices, and padding to avoid false sharing.

For the global MPMC queue, I am using a bounded lock-free implementation based on Dmitry Vyukov's sequence-number design. Each slot carries an atomic sequence counter that producers and consumers use to claim a slot without a separate lock. No external dependencies are required.

`push_batch()` is also worth implementing alongside `push()`. When a non-worker thread schedules a burst of tasks, it can push all of them and only update the atomic `bottom` index once, rather than once per task. This keeps the hot path for `steal()` from seeing a sequence of individual publishes.

***

***A downside however with the Task type I am using; the Task type is 16 bytes, this means loads/stores may not be lock-free unless you use 128 bit atomics. 

You have a few options;
- Enable the "-mcx16" flag, this enables 128 bit atomics.
	- The majority of 64 bit processors will support this, certain triplet targets may already enable it.
- Omit the std::atomic inside of the spmc buffer.
	- This isn't entirely correct, but a quite common approach.

I will be going with the option of enabling 128 bit atomics, as all the processors I target, support it. But this is a trade-off you should consider.

```cpp
namespace ytl::spmc
{

template <size_t Size>
concept PowerOfTwo = std::has_single_bit(Size);

static constexpr size_t CacheLine = 64u;

/*
 * A Cyclic buffer, where the elements are an atomic T using relaxed order operations.
 * The synchronization of indices, should be handled externally.
 */
template <typename T, size_t Capacity>
requires PowerOfTwo<Capacity>
class alignas(CacheLine) CyclicBuffer
{
  public:
    static constexpr size_t Size = Capacity;
    static constexpr size_t Mask = Capacity - 1;

    T
    get(const size_t index)
    {
        return _buffer[index & Mask].load(std::memory_order_relaxed);
    }

    void
    set(const size_t index, T item)
    {
        _buffer[index & Mask].store(std::move(item), std::memory_order_relaxed);
    }

  private:
    std::array<std::atomic<T>, Capacity> _buffer;
};

/*
 * Abort can be called when a race against a fellow stealer, or the owner is lost.
 * A retry in this case, may succeed.
 */
enum class StealError : uint8_t
{
    Empty,
    Abort,
};

/*
 * A Fixed-Size Single-Producer / Multi-Consumer (Chase-Lev) work-stealing Deque.
 *
 * Local operations are in a List in first out order,
 * while remote operations are First in first out.
 *
 * Memory ordering follows Le, Pop, Cohen & Pottier, "Correct and Efficient
 * Work-Stealing for Weak Memory Models"
 */
template <typename T, size_t Capacity = 1024>
requires PowerOfTwo<Capacity> && std::is_trivially_copyable_v<T>
class Channel final
{
  private:
    struct alignas(CacheLine) PaddedIndex
    {
        std::atomic<int64_t> value{0};
    };

    CyclicBuffer<T, Capacity> _buffer;

    PaddedIndex _bottom;
    PaddedIndex _top;

  public:
    Channel() = default;
    ~Channel() = default;

    Channel(const Channel &) = delete;
    Channel &
    operator=(const Channel &) = delete;

    Channel(Channel &&) = delete;
    Channel &
    operator=(Channel &&) = delete;

    /**
     * @brief Push an item onto the bottom of the deque.
     *
     * Only the owner thread should call this.
     *
     * @param item The item to push
     * @return true if successful, false if the deque is full
     */
    bool
    push(T item)
    {
        int64_t bottom = _bottom.value.load(std::memory_order_relaxed);
        int64_t top = _top.value.load(std::memory_order_acquire);

        if (bottom - top >= static_cast<int64_t>(Capacity))
        {
            return false;
        }

        _buffer.set(static_cast<size_t>(bottom), std::move(item));

        // Publish: the release store on bottom makes the preceding (relaxed)
        // slot store visible to any thief that acquire-loads this new bottom.
        _bottom.value.store(bottom + 1, std::memory_order_release);

        return true;
    }

    /**
     * @brief Push multiple items onto the bottom of the deque.
     *
     * Only the owner thread should call this. More efficient than
     * multiple individual push() calls as it updates bottom only once.
     *
     * @param items Pointer to items to push
     * @param count Number of items to push
     * @return Number of items actually pushed (may be less if deque fills)
     */
    size_t
    push_batch(std::span<const T> items)
    {
        const int64_t bottom = _bottom.value.load(std::memory_order_relaxed);
        const int64_t top = _top.value.load(std::memory_order_acquire);

        const int64_t free = static_cast<int64_t>(Capacity) - (bottom - top);
        const auto available = static_cast<size_t>(free < 0 ? 0 : free);
        const size_t to_push = std::min(items.size(), available);

        if (to_push == 0)
        {
            return 0;
        }

        for (size_t i = 0; i < to_push; ++i)
        {
            _buffer.set(bottom + static_cast<int64_t>(i), items[i]);
        }

        _bottom.value.store(bottom + static_cast<int64_t>(to_push), std::memory_order_release);

        return to_push;
    }

    /**
     * @brief Pop an item from the bottom of the deque.
     *
     * Only the owner thread should call this. Returns items in LIFO order.
     *
     * @return The item, or nullopt if the deque is empty
     */
    std::optional<T>
    pop()
    {
        int64_t bottom = _bottom.value.load(std::memory_order_relaxed) - 1;
        _bottom.value.store(bottom, std::memory_order_relaxed);

        // Full fence: orders the bottom store above against the top load below
        // (the Dekker-style handshake with steal()).
        std::atomic_thread_fence(std::memory_order_seq_cst);

        int64_t top = _top.value.load(std::memory_order_relaxed);

        std::optional<T> res = std::nullopt;

        if (top <= bottom)
        {
            T item = _buffer.get(static_cast<size_t>(bottom));

            if (top == bottom)
            {
                if (!_top.value.compare_exchange_strong(
                        top,
                        top + 1,
                        std::memory_order_seq_cst,
                        std::memory_order_relaxed
                    ))
                {
                    _bottom.value.store(bottom + 1, std::memory_order_relaxed);
                    return res;
                }
                _bottom.value.store(bottom + 1, std::memory_order_relaxed);
            }

            res = item;
            return res;
        }

        _bottom.value.store(bottom + 1, std::memory_order_relaxed);
        return res;
    }

    /**
     * @brief Steal an item from the top of the deque.
     *
     * Any thread can call this. Returns items in FIFO order (oldest first).
     *
     * @return The item, or StealError, abort is called if a race is lost, where a retry may
     * succeed.
     */
    std::expected<T, StealError>
    steal()
    {
        std::int64_t top = _top.value.load(std::memory_order_acquire);

        // Full fence: pairs with pop()'s fence so top and bottom are observed
        // in a single total order.
        std::atomic_thread_fence(std::memory_order_seq_cst);

        const std::int64_t bottom = _bottom.value.load(std::memory_order_acquire);

        if (top < bottom)
        {
            // Read before the CAS commits us to this index. This is safe
            // because we still hold top at this value; the owner cannot recycle
            // the slot until top advances past it.
            T item = _buffer.get(static_cast<std::size_t>(top));

            if (!_top.value.compare_exchange_strong(
                    top,
                    top + 1,
                    std::memory_order_seq_cst,
                    std::memory_order_relaxed
                ))
            {
                return std::unexpected(StealError::Abort);
            }

            return item;
        }

        return std::unexpected(StealError::Empty);
    }

    /**
     * @brief Get the approximate size of the deque.
     *
     * This is only an approximation since the deque can be modified concurrently.
     */
    [[nodiscard]] size_t
    size() const
    {
        int64_t bottom = _bottom.value.load(std::memory_order_relaxed);
        int64_t top = _top.value.load(std::memory_order_relaxed);
        return static_cast<size_t>(bottom > top ? bottom - top : 0);
    }

    /**
     * @brief Approximate emptiness (see `size()`)
     */
    [[nodiscard]] bool
    empty() const
    {
        return size() == 0;
    }

    static_assert(std::atomic<T>::is_always_lock_free, "The type T is not actually lock-free.");
};

} // namespace ytl::spmc
```

The static assert ensures that it doesn't end up being a mutex based channel, as otherwise it would largely defeat the point.