# Support Changing `user_id` On A Live Client

## What

Changing auth principal on a live Jazz client is currently unsupported. We need a focused follow-up
task that decides whether anonymous -> authenticated, authenticated -> anonymous, and
authenticated -> different-authenticated transitions should be supported, and if so, how to make
them correct for offline writes, queued sync payloads, and permission checks.

## Where

- `packages/jazz-tools/src/runtime/auth-state.ts`
- `packages/jazz-tools/src/runtime/db.ts`
- `packages/jazz-tools/src/runtime/client.ts`
- `crates/jazz-tools/src/sync_manager/inbox.rs`
- `crates/jazz-tools/src/sync_manager/sync_logic.rs`
- `crates/jazz-tools/tests/history_conflict.rs`
- auth examples and auth docs

## Steps to reproduce

1. Start a client with anonymous local auth.
2. Go offline and write data guarded by a policy like `owner_id = session.user_id`.
3. Change auth to a different principal, for example anonymous -> external JWT or `alice -> bob`.
4. Write more data as the new principal.
5. Reconnect and observe how queued writes are evaluated and replayed.

## Expected

One of these should be true, explicitly and consistently:

- live principal changes are supported, and queued/offline writes remain correct
- or live principal changes are rejected, and client recreation is the only supported path

## Actual

The codebase now rejects live principal changes and requires recreating the client, because earlier
exploration showed correctness hazards:

- local writes capture session/authorship at write time
- server-side row permission checks use the current connection session at replay time
- queued writes from one principal can therefore be evaluated under a different principal later
- persisted offline commits already have reconnect/replay gaps in some paths

## Priority

high

## Notes

### Scope

- same-principal JWT refresh is out of scope; that already works through `updateAuthToken(...)`
- this issue is only about changing `session.user_id` on a live client
- include anonymous <-> external transitions, not just `alice -> bob`
- cover both in-memory reconnects and persisted offline commits
- update examples/docs only after the runtime contract is settled

### Challenges

- permission checks currently happen against the connection's current session, not an original
  per-write principal
- replaying old writes under a new principal is a correctness bug for policies like
  `owner_id = session.user_id`
- reconnect currently re-queues from in-memory objects, while persisted offline commits have known
  architectural gaps
- even if replay worked, we still need a clear rule for authorship, visibility, and retry behavior
  after principal changes
- supporting this safely may require durable per-write principal metadata, replay partitioning, or
  a hard boundary that flushes/abandons queued work before switching identity
