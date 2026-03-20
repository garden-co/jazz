# Sync Protocol Reliability Gaps (MVP)

This note describes the main reliability gaps in the current HTTP/SSE sync path.

## Summary

Today, the sync path is good at "send this soon", but weak at "prove this arrived, in order, and recover if it did not."

The biggest problems are:

1. Client updates can be sent out of order.
2. The client forgets an outgoing message before it knows the server accepted it.
3. If one message is lost, later incremental sync can build on the false assumption that the server already has it.
4. The server already returns per-message results, but the Rust client does not read them.
5. Reconnecting the receive stream is stronger than reconnecting the send path, so a connection can look healthy while client-to-server state is still wrong.

In user terms, this means:

- a local change can look successful but never reach other devices
- a query subscription can stay active on the server after the client thought it unsubscribed
- a durability wait can stall because an acknowledgement was lost
- reconnect can restore live updates without actually repairing hidden divergence

## Current Model

At a high level, the current path looks like this:

```text
local write
  ->
runtime outbox
  ->
async callback
  ->
HTTP POST /sync
  ->
server inbox
  ->
server event stream to other clients
```

The important detail is where the "source of truth" disappears.

```text
local write
  ->
runtime outbox
  -- drained and cleared -->
async send task
  -- maybe succeeds, maybe fails -->
server
```

Once the runtime hands a message to the async send task, the protocol mostly acts as if the server will eventually receive it.

## 1. Outbound Messages Can Arrive Out of Order

The current Rust client starts one independent async task per outbound payload.

That means the client can produce changes in the right order but send them in a different order if one task is delayed by scheduling, network timing, or test delay hooks.

```text
Runtime produces:   A -----> B

Send tasks:         send(A) ----sleep-----> POST A
                    send(B) ------POST B--->

Server sees:        B first, A second
```

Why this matters:

- later updates can arrive before earlier updates that they logically depend on
- control messages can also reorder, not just data messages
- the protocol becomes timing-sensitive instead of state-driven

This is especially risky because the same path carries both object updates and control traffic like subscriptions, unsubscriptions, acknowledgements, and settlement notifications.

There is already an ignored regression test for this burst-ordering failure:
[`subscription_reflects_final_state_after_rapid_bulk_updates`](../../../crates/jazz-tools/tests/subscribe_all_integration.rs).

## 2. The Client Forgets Outbound Messages Too Early

The runtime drains its sync outbox before network success is known.

In plain language: we take the letter out of the outbox before the courier confirms pickup.

```text
Step 1: write is recorded locally
Step 2: runtime puts sync message in outbox
Step 3: batched tick drains and clears that outbox
Step 4: async HTTP send happens later
Step 5: if HTTP fails, there is no built-in resend record
```

Why this matters:

- a transient network failure can permanently drop a message
- a process crash between "outbox drained" and "server accepted" loses work from the sync path
- the only current reaction is mostly a warning log, not protocol recovery

This is the core reliability gap in the current design.

## 3. A Lost Message Can Poison Later Incremental Sync

The protocol keeps sender-side memory about what it believes the server has already seen.

That memory moves forward before delivery is confirmed.

```text
Client history:     C1 -----> C2

What client believes:
server has C1

What actually happened:
C1 was lost

Next incremental send:
client sends only "what changed after C1"

Server reality:
missing C1, receives later history anyway
```

This creates a sticky failure mode:

- the first lost message is bad
- later messages may stop including the missing history
- the system can remain divergent until some explicit full re-sync repairs it

An important amplifier here is that the receiver currently accepts incoming commits without clearly failing fast on missing parent history. So a dropped earlier update does not necessarily become a clean, visible error. It can turn into harder-to-diagnose inconsistent history instead.

## 4. The Server Reports Per-Message Results, but the Client Ignores Them

The `/sync` endpoint already returns a structured result for each payload:

```text
POST /sync
  ->
HTTP 200
  + response body:
    [ok, ok, failed, ok]
```

The current Rust transport checks the HTTP status code, but it does not read the response body that says whether each payload succeeded.

So these two cases currently look the same to the client:

- "the server accepted everything"
- "the server returned 200 but one or more payloads failed"

Why this matters:

- application errors can be hidden behind a successful HTTP status
- the client loses the chance to retry, resync, or surface a meaningful error
- the protocol already has a richer signal, but the client drops it on the floor

## 5. Reconnect Repairs the Receive Side Better Than the Send Side

The incoming event stream has an explicit reconnect loop and stream sequence handling.

The outgoing `/sync` path does not have a matching recovery story.

```text
server -> client stream
  has reconnect loop
  has stream sequence

client -> server POST /sync
  no delivery acknowledgement
  no resend queue
  no explicit replay on reconnect
```

Practical consequence:

- after a disconnect, the client can resume receiving fresh server updates
- but that does not prove the server received the client's earlier unsent or failed outbound messages

So the UI can look "reconnected" while client-to-server divergence is still present.

There is also implementation drift here: reconnect behavior is not driven by one shared state machine across clients, which makes it harder to state and test one protocol story.

## 6. Data Messages and Control Messages Share the Same Fragile Path

The sync pipe is not only row data.

It also carries control messages such as:

- query subscriptions
- query unsubscriptions
- persistence acknowledgements
- query-settled notifications

That means the same reliability gap affects both "the row value changed" and "the server should stop doing work for this query."

Simple examples:

```text
lost QuerySubscription
  -> server never starts sending a result set

lost QueryUnsubscription
  -> server keeps work alive after the client thinks it is done

lost PersistenceAck
  -> a durability waiter can wait longer than it should
```

This is why the problem is larger than "maybe one row update is late."

## Engineering Landmarks

These are the main code paths behind the issues above.

- Per-payload async send from the Rust client callback: `crates/jazz-tools/src/client.rs`
- Test hook showing send timing can be perturbed for `ObjectUpdated`: `crates/jazz-tools/src/client.rs`
- Runtime drains and clears the outbox before the network handoff completes: `crates/jazz-tools/src/runtime_core/ticks.rs`, `crates/jazz-tools/src/sync_manager/mod.rs`
- Sender-side `sent_tips` bookkeeping advances before delivery confirmation: `crates/jazz-tools/src/sync_manager/sync_logic.rs`
- Receiver applies incoming commits in `apply_object_updated()` / `receive_commit()`: `crates/jazz-tools/src/sync_manager/inbox.rs`, `crates/jazz-tools/src/object_manager/mod.rs`
- `/sync` returns `SyncBatchResponse` with per-payload results: `crates/jazz-tools/src/transport_protocol.rs`, `crates/jazz-tools/src/routes.rs`
- Rust client transport currently checks HTTP status but does not consume the response body: `crates/jazz-tools/src/transport.rs`
- Rust client stream reconnect loop and initial server registration: `crates/jazz-tools/src/client.rs`
- TS runtime/worker shared stream controller, showing the broader reconnect split across implementations: `packages/jazz-tools/src/runtime/sync-transport.ts`
