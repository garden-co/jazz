# Worker bridge should reuse core wire-frame batches

## What

The browser main-thread <-> worker bridge currently uses a bespoke `postMessage` path. A future cleanup should carry the same core wire-frame batches across worker and network boundaries, without reviving the removed alpha transport manager.

## Priority

low

## Notes

- Payoff: one core wire vocabulary across network and worker.
- Complication: the worker bridge is bidirectional `postMessage`, not a WebSocket. The semantics (no real "disconnect") need thought.
- Deferred follow-up after the core worker/server path settles.
