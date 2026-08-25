**Fast and Flexible Lock-Free C++ Job/Task-system with Co-routine Support.
***

**Outline
***

- Intro
	- Why?
	- Expected Knowledge
	- Setup Notes

- Task Unit
	- Cancellation (concept)

- Collections

- Scheduler
	- Waking & Sleeping

- Allocator
	- Cancellation (full implementation)

- Coroutines
	- Executor Switching

- Fibers

- Fork-Join Parallelism
	- Parallel Iterators
	- WaitGroup

- Results & Refinements


**Why would you want this?
***

With modern processors getting more and more cores, being able to efficiently take advantage of them has become very important for real-time applications such as games; though the same applies to any compute-heavy software.

In comparison, the single-threaded performance of processors has not risen as much.

Making use of the cores available to the processor allows for faster execution times and smoother frame pacing, since heavy computation can be offloaded from the main thread. IO operations, physics, animation sampling- these can all run in parallel, keeping the UI thread free so the user never experiences input lag.

Spawning a thread for every task is very inefficient: OS context switches involve multiple system calls and require saving and restoring all registers. For this you generally want a thread pool or other user-land task scheduler.

A simple approach is a global shared queue with a mutex and condition variable. However, when spawning many small tasks, this leads to significant lock contention and serializes all scheduling through a single bottleneck.

This is where lock-free data structures come in.


**Expected Knowledge**
***

C++ Experience
- This won't cover the basics of C++

Multi-threading Familiarity
- This won't go into the basics of multi-threading
- It won't go into the basics of atomics
- Optionally, some familiarity with C++ co-routines (or other stackless co-routines)


**Setup Notes
***

The code uses C++ 23.
- C++ 20 would also work; older versions may not support all features; for example co-routines and certain atomic operations. (like std::atomic::wait, C++ concepts and more)
- Some parts will use Clang-specific attributes; these are optional and will be explicitly marked as Clang-only.

While the code uses C++, the concepts may transfer to other languages as well.
For example, certain parts are directly C-compatible.

Note: this tutorial does not touch NUMA-aware scheduling.
This could however be added at a later time by extending the scheduler strategy.

**What you'll learn
***

- Type-erasure for tasks: a 16-byte `{void*, fn*}` pair that works for lambdas, coroutines, fibers, and stack-allocated tasks without changing the scheduler.
- Writing extensible cooperative schedulers: a `Scheduler` concept that lets you swap in work-stealing, shared queues, or any other strategy.
- Lock-free memory allocation: a per-worker SPMC slab allocator that eliminates contention on the global heap and keeps allocations in cache.
- Integrating stackless coroutines: custom frame allocation, symmetric transfer, and parallel co-routine joins.
- Stackful fibers via Boost.Context: slotting fibers into the same task abstraction without modifying the scheduler or worker loop.
- Fork-join parallelism and parallel iterators: divide-and-conquer over spans and index ranges with zero heap allocation.
