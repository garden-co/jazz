# Dedicated tick thread (status quo)

## Problem

`batched_tick()` acquires `Arc<Mutex<RuntimeCore>>` and does significant synchronous work: processing sync messages, flushing WAL, firing subscription callbacks. When this ran inside `tokio::spawn`, it blocked a tokio worker thread for the duration.

Other async tasks — SSE streams, health checks, HTTP handlers — also need the mutex. With multiple connected clients, every tokio worker thread ends up parked on the mutex while the tick holds it. Result: deadlock.

## Solution

A dedicated OS thread spawned per `TokioRuntime` instance. It parks on `std::sync::mpsc::channel::recv()` waiting for work. `schedule_batched_tick()` sends a `()` wake-up signal. The thread wakes, drains extra notifications (natural debouncing), acquires the mutex, runs `batched_tick()`, releases the mutex, and parks again.

The thread is spawned in `TokioScheduler::set_core_ref()` once the `Weak<Mutex<RuntimeCore>>` is available.

## Trade-offs

**Pros:**

- No tokio worker threads blocked — the dedicated thread is outside the async runtime
- No thread-pool scheduling overhead (unlike `spawn_blocking`)
- Thread is always warm — no cold-start latency
- Natural debouncing via channel drain before processing

**Cons:**

- One extra OS thread per `TokioRuntime` instance (trivial)
- `batched_tick()` still runs synchronously — large sync payloads still take wall-clock time, just on a dedicated thread
- Clean shutdown depends on `Arc<Mutex<RuntimeCore>>` being dropped to drop the `Sender` and unblock the thread's `recv()`
