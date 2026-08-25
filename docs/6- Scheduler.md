***

Let's get started with the scheduler and the actual ThreadPool. This won't be a too in-depth section.

For flexibility I'll be using a templated strategy for the scheduling algorithm, this allows you to switch out the work stealing scheduler for a shared global queue or other options, if desired.

Here is the concept for the strategy.

```cpp
template <typename T>
concept Scheduler = requires(T sched, Task task) {
    { sched.init(size_t{}) };
    { sched.schedule(task) };
    // Blocks on the call until either a task is available or shutdown is called.
    { sched.next() } -> std::same_as<std::optional<Task>>;
    { sched.try_next() } -> std::same_as<std::optional<Task>>;
    { sched.has_ready() } -> std::same_as<bool>;
    { sched.shutdown() };
};
```

Here is the work stealing strategy. It uses per thread meta-data, which includes a local Task channel, it also includes things such as the semaphore and the allocator.

Before going back to sleep when a worker runs out of work, it will perform an exponential back-off, since context switches are quite expensive, this can help significantly when tasks are being frequently spawned.

```cpp

#ifndef YTL_WORKER_BLOCK_SIZE
#define YTL_WORKER_BLOCK_SIZE 512
#endif

namespace ytl
{

using Task = task::TaskRef;
using LocalQueue = spmc::Channel<Task, 8192>;
using GlobalQueue = mpmc::Channel<Task, 8192>;

class WorkStealing
{
  public:
    struct WorkerData
    {
        alignas(64) LocalQueue local_queue;

        spmc::Allocator allocator{spmc::AllocatorConfig{.block_size = YTL_WORKER_BLOCK_SIZE}};

        std::binary_semaphore semaphore{0};

        WorkerData() = default;
        ~WorkerData() = default;

        WorkerData(const WorkerData &) = delete;
        WorkerData(WorkerData &&) = delete;

        WorkerData &
        operator=(const WorkerData &) = delete;

        WorkerData &
        operator=(WorkerData &&) = delete;
    };

    void
    init(size_t count)
    {
        std::construct_at(&waker_, &WorkStealing::wake_callback, this);
        workers_.reserve(count);
        for (size_t i = 0; i < count; ++i)
        {
            workers_.emplace_back(std::make_unique<WorkerData>());
        }
    }

    void
    schedule(Task task)
    {
        size_t idx = current_worker_idx();
        bool pushed = false;

        if (idx != SIZE_MAX)
        {
            pushed = workers_[idx]->local_queue.push(task);
        }

        if (!pushed)
        {
            global_queue_.try_push_bounded(task);
        }

        waker_.wake(1);
    }

    [[nodiscard]] std::optional<Task>
    next()
    {
        constexpr size_t ROUNDS_UNTIL_SLEEP = 32;
        size_t idle = 0;

        while (true)
        {
            if (auto task = try_next())
            {
                return task;
            }

            if (stop_.load(std::memory_order_acquire))
            {
                return std::nullopt;
            }

            if (idle < ROUNDS_UNTIL_SLEEP)
            {
                ++idle;
                pause();
            }
            else
            {
                size_t idx = current_worker_idx();
                if (idx != SIZE_MAX)
                {
                    waker_.mark_sleeping(idx);

                    // Re-check after marking to avoid missing work submitted between
                    // try_next() and mark_sleeping().
                    if (auto task = try_next())
                    {
                        waker_.mark_awake(idx);
                        return task;
                    }

                    if (stop_.load(std::memory_order_acquire))
                    {
                        waker_.mark_awake(idx);
                        return std::nullopt;
                    }

                    workers_[idx]->semaphore.try_acquire_for(std::chrono::milliseconds(50));
                    waker_.mark_awake(idx);
                }
                else
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }

                idle = 0;
            }
        }
    }

    [[nodiscard]] std::optional<Task>
    try_next()
    {
        size_t idx = current_worker_idx();

        if (idx != SIZE_MAX)
        {
            if (auto task = workers_[idx]->local_queue.pop())
            {
                return task;
            }
        }

        if (auto task = global_queue_.try_pop())
        {
            return task;
        }

        return steal(idx);
    }

    [[nodiscard]] bool
    has_ready()
    {
        size_t idx = current_worker_idx();
        if (idx != SIZE_MAX && !workers_[idx]->local_queue.empty())
        {
            return true;
        }
        return !global_queue_.empty();
    }

    void
    shutdown()
    {
        stop_.store(true, std::memory_order_release);
        waker_.wake_all();
    }

    [[nodiscard]] IAllocator *
    allocator()
    {
        if (IWorker *worker = IWorker::current())
        {
            return &workers_[worker->index()]->allocator;
        }
        return &external_allocator_;
    }

  private:
    static void
    wake_callback(size_t idx, void *ptr)
    {
        auto *self = static_cast<WorkStealing *>(ptr);
        assert(idx < self->workers_.size());
        self->workers_[idx]->semaphore.release();
    }

    [[nodiscard]] static size_t
    current_worker_idx()
    {
        if (IWorker *worker = IWorker::current())
        {
            return worker->index();
        }
        return SIZE_MAX;
    }

    [[nodiscard]] size_t
    random_idx() const
    {
        assert(!workers_.empty());
        return tl_rng_() % workers_.size();
    }

    [[nodiscard]] std::optional<Task>
    steal(size_t self_idx)
    {
        std::optional<Task> res = std::nullopt;
        if (stop_.load(std::memory_order_acquire) || workers_.size() <= 1)
        {
            return res;
        }

        constexpr size_t MAX_ATTEMPTS = 3;

        for (size_t attempt = 0; attempt < MAX_ATTEMPTS; ++attempt)
        {
            size_t victim = random_idx();
            if (victim == self_idx)
            {
                continue;
            }

            auto steal_result = workers_[victim]->local_queue.steal();
            if (steal_result)
            {
                res = *steal_result;
                return res;
            }
        }

        return res;
    }

    // NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
    inline static thread_local std::minstd_rand tl_rng_{std::random_device{}()};

    std::vector<std::unique_ptr<WorkerData>> workers_;
    GlobalQueue global_queue_;
    mpmc::Allocator external_allocator_;
    AtomicWaker waker_;
    std::atomic<bool> stop_{false};
};

} // namespace ytl
```

***

Two details that complete the picture.

The `WorkerData` allocator lookup depends on thread-local state. Each worker calls `IWorker::set_local(this)` at the start of its run loop and `IWorker::set_local(nullptr)` when it exits. This is what lets `allocator()`, `current_worker_idx()`, and later the coroutine frame allocator find the correct per-thread state without any locking.

```cpp
void Worker::run()
{
    IWorker::set_local(this);
    while (auto task = sched_.next())
    {
        task->execute();
    }
    IWorker::set_local(nullptr);
}
```

The `ThreadPool` also exposes a `help_until(predicate)` method. When a non-worker thread (or the main thread) needs to wait for a task to complete, rather than blocking, it calls `try_next()` in a loop- stealing and executing real work until the predicate is satisfied. 
This is what the fork/join operations in the later section rely on. 

```cpp
template <typename Predicate>
requires std::is_invocable_r_v<bool, Predicate>
void help_until(Predicate &&pred)
{
    int spins = 0;
    while (!std::invoke(std::forward<Predicate>(pred)) &&
           !_shutdown.load(std::memory_order_acquire))
    {
        if (auto job = sched_.try_next())
        {
            job->execute();
            spins = 0;
        }
        else
        {
            if (spins < 64)
            {
                ++spins;
                pause();
                continue;
            }
            std::this_thread::yield();
            spins = 0;
        }
    }
}
```

The `_shutdown` check is important: without it, `help_until` would spin forever if the thread pool is stopped while a join is in progress. The pause/yield backoff mirrors the `next()` loop; `pause()` first to avoid saturating the memory bus, then `yield()` after 64 spins to relinquish the CPU to the OS scheduler.