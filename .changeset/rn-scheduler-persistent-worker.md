---
"jazz-rn": patch
---

Stop the React Native scheduler from spawning a thread per tick. Every accepted tick or mutation-error delivery used to spawn a fresh thread that blocked until the JS thread serviced its callback, and because the debounce flag was released before the callback returned, a heavy burst of bare inserts minted roughly one blocked thread per millisecond. Jobs are now queued to a single lazily-spawned worker thread, so the scheduler's thread count stays constant and callbacks are serialized. Shutdown from `close()` drops the worker's channel instead of joining it (a worker blocked on a JS callback would deadlock the JS thread running `close()`), the tick callback is no longer invoked while holding the mutex that `close()` needs, a failed worker spawn no longer poisons the scheduler, and a panicking mutation-error callback can no longer kill the worker.
