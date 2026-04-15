# Worker never emits `auth-failed` on server JWT rejection

## What

When the WebSocket server rejects a JWT, the worker needs to post `{ type: "auth-failed", reason }` back to the main thread so `WorkerBridge` can drive `getAuthState().status` to `"unauthenticated"`. The message type is already defined in `WorkerToMainMessage` and handled in `WorkerBridge`, but `jazz-worker.ts` never emits it. As a result, the main thread never learns of a server-side auth rejection and the auth state machine cannot transition from that path.

## Priority

high

## Notes

- Reverse direction of the auth flow (server → main thread) complementary to the outbound refresh path fixed in `specs/todo/a_mvp/transport_control_and_worker_wiring.md` (outbound direction: main thread → Rust transport).
- Surfaced during T23 of that spec's implementation plan while writing the E2E worker-path auth refresh test; noted there that the existing `db.auth-refresh.test.ts` also fails for this reason when exercised via `workerBridge`.
- Likely requires the Rust transport to surface a "handshake rejected" signal up through `TransportInbound` (or similar) so the worker can translate it into the `auth-failed` postMessage.
