***

With a basic C++ project set up, let's create the task type. This will be the unit that the thread pool actually executes. You have multiple options here.

Some include:
- `std::move_only_function` (or `std::function` if you need copyability)
- `std::function_ref`
- abstract interface pointers
- `void*` + trampoline
- ...

I will be going with a `void*` and trampoline, as this provides a lot of flexibility in terms of what the task is and where it's allocated.
Additionally it gives the task direct C compatibility, which is a bonus for my use case.

You can also go with the interface approach if that is preferred, though this will make certain parts a bit less efficient later on during the co-routine and fiber parts.

`std::function` and friends would not be optimal, as they don't provide much control over how the callable is allocated when it doesn't fit in the inline buffer, and don't give control over extra fields or metadata per task.

A generic `std::coroutine_handle<>` is also an option.
This may simplify some things in the worker loop, but makes stack-allocating tasks much more tedious and can have more overhead in things such as parallel iterators.
It also has the same allocation control downsides as the `std::function` family.

Here is the task type I will be using:

```cpp
using TaskFn = void(*)(void*);

struct TaskRef
{
    void*  ptr;
    TaskFn function;

    void execute() const { function(ptr); }
};
```

Here is a concrete implementation of a task that stores an invocable and arguments:

```cpp
template <std::invocable T>
auto
make_trampoline(T* invocable)
{
    return [](void* ptr)
    {
        T* invocable = static_cast<T*>(ptr);
        (*invocable)();
    };
}

template <std::invocable T>
TaskRef as_task(T* ptr)
{
    return { .ptr = ptr, .function = make_trampoline<T>() };
}


template <typename T, typename Destroyer, typename... TArgs>
requires std::is_invocable_v<T, TArgs...>
class InvocableTask final
{
  public:
    template <typename U, typename... UArgs>
    requires std::is_same_v<std::decay_t<U>, T> &&
                 (std::is_same_v<std::decay_t<UArgs>, TArgs> && ...)
    InvocableTask(U &&invocable, UArgs &&...args) :
        invocable_(std::forward<U>(invocable)),
        args_(std::make_tuple(std::forward<UArgs>(args)...))
    {
    }

    void
    operator()()
    {
        std::apply(std::move(invocable_), std::move(args_));
        // If you want to execute it multiple times, use a no-op Destroyer.
        Destroyer{}(this);
    }

  private:
    // You can still store a reference using std::ref,
    // but take caution with lifetime.
    std::tuple<std::decay_t<TArgs>...> args_;
    T invocable_;
};
```

Task deletion- since the task can be stack or heap allocated, we use a `Destroyer` type: an empty struct with a call operator for a pointer. This allows a custom allocator to be slotted in later; for now it redirects to `delete`. It also allows deletion to be omitted for stack-allocated tasks or compiler-managed frames (co-routines). The compiler can easily inline this, so you get no overhead regardless.

```cpp
struct TypedDestroyer
{
    template <typename T>
    void
    operator()(T *ptr) const
    {
        // NOLINTNEXTLINE(cppcoreguidelines-owning-memory)
        delete ptr;
    }
};

struct NoOpDestroyer
{
    template <typename T>
    void
    operator()(T * /*unused*/) const
    {
    }
};
```

Usage:

```cpp
#include <print>

template <typename T, typename... TArgs>
requires std::is_invocable_v<T, TArgs...>
auto *
make_task(T &&invocable, TArgs... args)
{
    using task_t = InvocableTask<std::decay_t<T>, TypedDestroyer, std::decay_t<TArgs>...>;
    return new task_t(std::forward<T>(invocable), std::forward<TArgs>(args)...);
}

int main()
{
    auto* heap_task = make_task([]{ std::println("From Task."); });
    TaskRef task = as_task(heap_task);

    task.execute();
    return 0;
}
```

One problem with the shown `InvocableTask`: if not executed, it will leak memory.
If that is a concern for your use case, you could add a destroy trampoline to the task type. For my personal use, this isn't required, so I will omit it.

A stack-allocated version:

```cpp
#include <print>

template <typename T, typename... TArgs>
requires std::is_invocable_v<T, TArgs...>
auto
make_stack(T &&invocable, TArgs &&...args)
{
    using task_t = InvocableTask<std::decay_t<T>, NoOpDestroyer, std::decay_t<TArgs>...>;
    return task_t(std::forward<T>(invocable), std::forward<TArgs>(args)...);
}

int
main()
{
    auto stack = make_stack([] { std::println("From Stack"); });
    auto s_ref = as_task(&stack);
    s_ref.execute();

    return 0;
}
```

I will be using continuations in the form of co-routines, which will show up later. You can also use continuations through `.then` chains. If desired, you can store a continuation collection (such as an atomic linked list) in the `InvocableTask` class.

A big benefit with this specific task type: when using co-routines, the compiler allocates the co-routine frame for you. It creates a `std::coroutine_handle<>`, which is essentially just a `void*`. We can directly store this inside of the `TaskRef` type, so no extra allocation is incurred beyond the co-routine frame itself. If we were to go with an interface, we would get the co-routine allocation plus an additional interface allocation.

***

**Simplification with an allocator**

`InvocableTask` stores all arguments inline via `std::tuple`, which works but has a wide template signature and requires knowing the argument types upfront. Once we have the per-worker allocator (section 7), we can simplify some things.

The actual task type used throughout the rest of this series is `HeapTask<Body, Alloc>`:

```cpp
template <typename Body, Allocator Alloc, typename Destroyer = AllocatorDestroyer>
class HeapTask final
{
    Body  body;
    Alloc *alloc;

  public:
    HeapTask(Alloc *alloc, Body &&body) : body(std::move(body)), alloc(alloc) {}

    [[nodiscard]] TaskRef as_ref() { return TaskRef::create(this); }

    void operator()()
    {
        body();
        Destroyer{}(this);
    }
};
```

`HeapTask` stores a single callable (the body) and a pointer to the allocator it came from. No `std::tuple`, no variadic template arguments, if you do want support for arguments, you can easily add it back with the same std::tuple approach as the previous. `AllocatorDestroyer` (the default `Destroyer`) routes deallocation to `deallocate` if the executing thread owns the allocator, or `ts_deallocate` if it is a thief thread. 
This replaces `TypedDestroyer` from the examples above.

The `InvocableTask` above and the `Destroyer` pattern are the conceptual foundation- the rest of the series uses `HeapTask`.

***Note: if your functions can throw exceptions, add a try-catch inside the call operator to ensure the task doesn't leak upon exceptions.
