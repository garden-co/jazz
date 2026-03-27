# Scopes: Client State Lifecycle

## Scope 1: Disconnect candidates set

Add `disconnect_candidates: RwLock<HashMap<ClientId, Instant>>` to `ServerState`. Wire up the two entry points:

- **SSE stream close** (`events_handler` cleanup block): scan `connections` for remaining streams with this `client_id`. If none → insert into candidates with `Instant::now()`.
- **Client reconnect** (`events_handler` connect path): remove `client_id` from candidates if present.

Rename `_client_id` to `client_id` on `ConnectionState` (it's already there but unused).

**Verification:** Unit tests on `ServerState` directly:

- alice connects, disconnects → in candidates
- alice connects, disconnects, reconnects → not in candidates
- alice has two connections, one closes → not in candidates
- alice has two connections, both close → in candidates

**No sweep yet.** Candidates accumulate but nothing reaps them.

---

## Scope 2: Periodic sweep

Background `tokio::interval(30s)` task that:

1. Write-locks `disconnect_candidates`, drains expired entries (where `disconnected_at + ttl < now`)
2. Unlocks candidates
3. For each drained ID: read-lock `connections`, check for active connection → if none, call `runtime.remove_client(id)`

Add `client_ttl: Arc<AtomicU64>` to `ServerState` (default 5 min). Sweep reads it each tick.

**Verification:** Integration tests with controllable time (`tokio::time::pause`):

- alice disconnects, time advances past TTL, sweep fires → state reaped
- alice disconnects, time advances but below TTL, sweep fires → state preserved
- alice disconnects, sweep drains candidates, alice reconnects before reap → connection check catches it, state preserved
- bob unaffected when alice is reaped
- sweep processes multiple expired candidates in one tick

---

## Scope 3: Runtime-adjustable TTL

Expose a setter for `client_ttl` (admin API endpoint or method on `ServerState`). The sweep already reads it each tick, so this is just the mutation path.

**Verification:**

- Set TTL to 1s, verify faster reaping
- Change TTL at runtime, verify next sweep uses new value

---

## Dependency graph

```
Scope 1 (candidates set)
    │
    ▼
Scope 2 (sweep)
    │
    ▼
Scope 3 (runtime TTL) — optional, can ship without
```

Scopes 1 and 2 are the core. Scope 3 is a small addition that can land later.
