# Plan: fail-closed claim epochs and live subscription rebinding

Status: minimized public-API security reproduction. No production worktree
change.

## Security invariant

After trusted session claims change, every active subscription must reflect the
new authorization context before it can emit another protected row. A revoked
stream cannot retain old rows or receive future writes.

## Evidence

1. Set `isAdmin=true` for a non-system identity.
2. Subscribe to an admin-claim-protected document query.
3. Call `set_identity_claims(identity, {isAdmin: false})`.
4. A new one-shot immediately returns zero, proving the current claim map
   changed.
5. The live subscription emits no removal.
6. Insert another matching document.
7. The stale live stream emits the new document as `added`.

This is a post-revocation data leak, not only stale client state. Membership-row
revoke/restore is a positive control and emits exact remove/add deltas; the
failure is specific to mutation of session-claim inputs.

## Source

- `crates/jazz/src/db.rs:1927-1936` updates the session claim map and queues an
  upstream `SessionClaims` message.
- Existing local maintained subscriptions retain their original bound claim
  tuple.
- No local reset, close, rebind, or authorization-epoch invalidation follows.
- The receiving `SessionClaims` path similarly updates trusted claims without
  rebuilding already-served subscription bindings.

## Proposed model

Give every identity an immutable claim digest and monotonically increasing
claim epoch. This is binding-instance/lifecycle state, not part of the shared
prepared-graph identity. Store the revision on:

- local maintained subscriptions;
- served/upstream subscriptions;
- bound executions and settled result sets;
- permission/read/write-policy caches;
- reconnect/resume coverage state.

Shared compiled plans remain keyed by policy identity and claim-path/parameter
signature. They must bind claim values at runtime as required by
`SPEC/14_lowering_to_groove.md`; an epoch selects/replaces a binding instance,
not a new compiled graph.

On a claim update, fail closed immediately.

Preferred transition:

```text
accept trusted new claims
  -> create next epoch
  -> open/reseed next-epoch maintained binding
  -> diff old and new authorized snapshots
  -> atomically publish reset/delta and swap bindings
  -> detach every old-epoch binding/cache entry
```

Acceptable first security milestone:

```text
claim epoch changes
  -> close/reject every affected stream
  -> detach old state
  -> require explicit resubscribe
```

Silently continuing with the old binding is never valid.

## Implementation phases

### C1: claim revision identity

- Canonically encode trusted claims and hash them.
- Increment an identity-scoped epoch for every accepted semantic change.
- Treat expiry/TTL as the same epoch-changing operation.
- Reuse an epoch only when canonical claims are exactly unchanged.

### C2: local fail-closed invalidation

- Index live local subscriptions by identity/context.
- On epoch change, close them before another write can refresh old bindings.
- Invalidate old-epoch bound results and permission decisions. Reuse a compiled
  plan only after rebinding the new runtime claim values.
- Compose cleanup with local Groove unsubscribe.

### C3: atomic local rebind

- Seed the new binding at a defined logical frontier.
- Diff old/new snapshots into one consolidated event.
- Swap only after seeding succeeds; on error, keep the stream closed.
- Handle a concurrent write under exactly one epoch—never both or neither.

### C4: serving and reconnect paths

- Apply the same epoch transition after a trusted upstream `SessionClaims`
  update.
- Include context/epoch in resume and coverage keys.
- Reject stale-epoch messages and prevent reconnect from restoring old grants.
- Define claim assertion trust/admission changes as context changes.

## Gates

- `true -> false`: existing rows remove or stream closes; later rows never
  arrive;
- `false -> true`: current rows add exactly once and later rows arrive;
- arbitrary claim value A -> B;
- local-only, client/edge/server, and trusted-backend paths;
- claim update concurrent with document commit;
- reconnect/resume cannot resurrect old epoch;
- one-shot, subscription, `can_read`, and write-policy decisions agree;
- claim expiration behaves like explicit revocation;
- unchanged canonical claims create no reset/churn.

Add counters for affected streams, rebind success/failure, stale-epoch rejection,
and old-epoch state retained after transition. A gate should assert zero old
bindings after quiescence.

## Dependencies

Complete prepared claim parameterization/routing should land with or before
this plan. Explicit local output teardown is also required so a closed revoked
stream cannot survive as a stale Groove output.

Tooling friction: claims need a test clock and a public stream-state/epoch
diagnostic to make expiry and concurrent-transition tests deterministic.
