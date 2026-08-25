***

Now it's time to get started on the allocation strategies.

You could use new/delete directly, but this can be quite expensive, especially during heavy multi-threaded usage.

The main problem is contention on the global allocator. When multiple threads call `::operator new` simultaneously, they serialize on the global heap's internal lock. Beyond the lock itself, freshly allocated memory is unlikely to be in the worker's L1 cache, so every task allocation risks a cache miss. For a system that may spawn thousands of short-lived tasks per second, this cost compounds quickly.

***

**Block allocator**

The approach I'm using is a slab/block allocator: request large chunks of memory from an upstream `pmr::memory_resource` up front, then carve them into fixed-size blocks. Allocation and deallocation are O(1), pop from or push onto a free list.  One downside this has, is that there aren't multiple block sizes, you could advance this to a very where you maintain different size buckets.

The configuration:

```cpp
struct AllocatorConfig
{
    size_t block_size = 1024;
    size_t blocks_per_chunk = 1024;
    size_t alignment = alignof(std::max_align_t);
    size_t start_chunks = 8;

    std::pmr::memory_resource *upstream = std::pmr::get_default_resource();
};
```

`block_size` is the fixed allocation unit. Every request for any size up to `block_size` returns the same block. This means you need to size it to the largest allocation you expect- in our case, a co-routine frame or a `HeapTask`.

`start_chunks` pre-allocates chunks at construction time, so the first few hundred tasks don't pay for chunk allocation at all.

***

**The SPMC split**

In a work-stealing scheduler, a worker thread allocates its own tasks, but a thief thread may execute -and therefore deallocate- a stolen task. So allocation is single-threaded, but deallocation can come from any thread.

You could protect the free_list with a mutex, or an mpsc channel, but this comes with more overhead than required in most cases. The better approach is two separate free lists:

- `free_list`; a regular pointer, no atomics. Only the owning worker touches this for both allocation and deallocation.
- `c_free_list`; an `atomic<Node*>`. Thief threads push blocks onto this list using a CAS loop.

```cpp
class Allocator final : public IAllocator
{
    Node *free_list{};
    std::atomic<Node *> c_free_list;
    // ...
};
```

When the owner thread deallocates (same thread as allocation), it uses the fast path:

```cpp
void deallocate(void *ptr, size_t /*bytes*/, size_t /*align*/) noexcept override
{
    Node *node = static_cast<Node *>(ptr);
    node->next = free_list;
    free_list = node;
}
```

When a thief thread deallocates (cross-thread, e.g. after stealing and executing a task), it uses the thread-safe path:

```cpp
void ts_deallocate(void *ptr, size_t /*bytes*/, size_t /*align*/) noexcept override
{
    Node *node = static_cast<Node *>(ptr);
    Node *head = c_free_list.load(std::memory_order_relaxed);
    do {
        node->next = head;
    } while (!c_free_list.compare_exchange_weak(
        head,
        node,
        std::memory_order_release,
        std::memory_order_relaxed
    ));
}
```

The release on the successful CAS ensures that the write to `node->next` is visible to whoever reads this node later.

***

**Harvesting in a single instruction**

The owner never needs to look at `c_free_list` during individual deallocations. It only harvests when it needs more blocks, specifically, when `free_list` runs dry during `allocate()`:

```cpp
void *allocate(size_t bytes, size_t align) noexcept override
{
    if (free_list == nullptr)
    {
        harvest_concurrent_blocks();
        if (free_list == nullptr)
        {
            alloc_chunk();
        }
    }

    Node *node = free_list;
    free_list = node->next;
    return node;
}

void harvest_concurrent_blocks() noexcept
{
    Node *list = c_free_list.exchange(nullptr, std::memory_order_acquire);
    if (list != nullptr)
    {
        free_list = list;
    }
}
```

`c_free_list.exchange(nullptr, acquire)` atomically takes the entire concurrent list in one instruction, no CAS loop, no iteration. The acquire ordering ensures all the `node->next` writes from `ts_deallocate` are visible. After the exchange, `free_list` points to a chain of blocks that can be consumed one by one without any further synchronization.

If the concurrent list was also empty, `alloc_chunk()` requests a new chunk from the upstream resource and links its blocks into `free_list`.

***

**Integration**

Each worker in `WorkStealing` holds its own allocator instance:

```cpp
struct WorkerData
{
    alignas(64) LocalQueue local_queue;
    spmc::Allocator allocator{spmc::AllocatorConfig{.block_size = YTL_WORKER_BLOCK_SIZE}};
    std::binary_semaphore semaphore{0};
};
```

The `ThreadPool::allocator()` method returns the current worker's allocator, or an external MPMC allocator if called from a non-worker thread:

```cpp
IAllocator *allocator()
{
    if (IWorker *worker = IWorker::current())
    {
        return &workers_[worker->index()]->allocator;
    }
    return &external_allocator_;
}
```

The `IAllocator` interface separates the single-threaded `allocate`/`deallocate` from the thread-safe `ts_create`/`ts_destroy`/`ts_deallocate`. Callers that know they're on the owning thread use the fast path; cross-thread callers (or the task's `Destroyer`) use the `ts_` variants. This distinction is what lets the allocator be lock-free end-to-end.