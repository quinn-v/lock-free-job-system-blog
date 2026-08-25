***

We'll start with a basic C++ 23 template. 
Choose your favorite build system and compiler, I will be using G++ 16.1 / Clang++ 22.1.6 with xmake.

If there is a compiler specific section, this will be noted. (Notably during the Co-routine section, due to clang's specific co-routine attributes).

There are a few optional dependencies- they will arise later in the tutorial. 
I would recommend setting up whichever package manager you prefer, these can however also be omitted entirely, by not implementing that feature, or rolling your own.

I will be using xmake's built-in package manager.

```lua
set_languages("c++23")

target("yatl")

    set_kind("static")
    add_files("src/**.cpp")

    add_includedirs("include", { public = true })

target_end()
```

***

One flag worth enabling now: `-mcx16`. 
This unlocks 128-bit compare-and-swap on x86-64, which the work-stealing deque relies on for lock-free 16-byte `Task` loads and stores. The majority of modern 64-bit processors support it.

```lua
add_cxxflags("-mcx16")
```

If you are targeting an architecture where this is already implied (some target triplets enable it by default), you can skip it.

If you'd like to avoid using this, you can also replace the std::atomic values in the buffer of the spmc channel with regular values, this is quite commonly done, however not technically correct.

***

Aside from a few optional dependencies, such as Boost.Fiber in the Fibers segment. There are no external dependencies.