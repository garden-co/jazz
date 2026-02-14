# Scheduled WAL flush

Currently `flush_wal()` runs at the end of every `batched_tick`. This is correct but wasteful — a single keystroke can trigger multiple ticks.

## Goal

Debounce WAL flushes so they happen at most every N ms (e.g. 500ms or 1s) rather than on every tick.

## Approach

Extend the `Scheduler` trait with a `schedule_wal_flush(&self)` method. The WASM scheduler implementation would debounce this via `setTimeout`, similar to how `schedule_batched_tick` works. The native scheduler can flush synchronously or use a background thread.

`batched_tick` calls `schedule_wal_flush()` instead of `flush_wal()` directly. The scheduler fires the actual flush after the debounce window.

## Notes

- `flush_wal()` is cheap (append buffer → OPFS) but still I/O — debouncing avoids redundant writes
- `flush()` (full snapshot) remains explicit (shutdown only, or periodic on a longer cadence)
- The current every-tick flush is a stopgap to unblock persistence debugging
