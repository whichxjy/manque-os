# Manque-OS

Manque-OS is a multitasking "operating system" written in pure Lua, inspired by David Beazley's tutorial [Curious Course on Coroutines and Concurrency](https://youtu.be/Z_OAlIhXziw).

## Notes

 - A task is a wrapper around a coroutine. The scheduler pulls tasks off the queue and runs them to the next yield.
 - Each task runs until it hits the yield. At this point, the scheduler regains control and switches to the other task.
 - In a real operating system, traps are how application programs request the services of the operating system (syscalls). In this program, the scheduler is the operating system and the yield statement is a trap.
 - To request the service of the scheduler, tasks will use the yield statement with a value.
 - Tasks do not see the scheduler. Tasks do not see other tasks. Yield is the only external interface.
