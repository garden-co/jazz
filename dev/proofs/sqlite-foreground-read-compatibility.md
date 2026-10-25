# SQLite foreground read compatibility

The WASM `setNonDurableClient()` entry point now marks an in-memory foreground
that reads through a storage owner. Previously it rejected every call. A fresh
tab must receive its owner's answer before publishing a local-first opening;
its own empty memory is not a substitute for the worker's cached data. Marking
the foreground also establishes its replica role before the worker connects.

## Owner and authority answers

A relay can now complete a subscription from its cache at `Local`. It upgrades
that subscription to `Global` only when its upstream reports global settlement.
The upgrade is delivered even when no rows change, including an empty authority
answer. Its response carries the upstream sequence it has observed, never the
relay's optimistic transaction counter. A global marker carrying an optimistic
sequence is rejected as a protocol error.

For example, Alice's worker holds a cached note while its authority is silent:

- An immediate local-first foreground read waits for the worker, then returns
  the cached note at Local.
- A foreground read with `FirstLoad::WaitForRemote` holds the worker's answer
  until global settlement, the requested timeout, the connection-attempt window,
  or a host-reported loss of the authority link. Fallback uses the owner's cache.
- A foreground Remote subscription still requires global settlement. A local
  reply, timeout or Failed hint cannot turn that reply into a global answer.
- A foreground LocalOnly read retains its explicit local-memory semantics.

Ordinary durable clients retain their existing Remote read contract. Foreground
subscriptions use the normal sync, query and session-policy machinery; this
change does not add a privileged data path. Large-value openings wait for the
chunks needed by the row, even when the authority link is offline.

One-shot foreground reads use a temporary normal subscription for the same
owner/authority distinction, then release it. Refusal and cancellation also
release its attachment. One-shot requests with a different served identity or
request-scoped claims remain explicitly unsupported on this foreground path;
trusted backend scoped reads keep their existing implementation.

## Host deadlines

The core now implements its tick-scheduler hook. Pending reads retain transient
deadline guards, including the connection-attempt boundary. Every tick re-arms
the next deadline because hosts may retain only their earliest timer. Cancelling
an earlier read cannot strand a later one. Completed or cancelled reads stop
retaining their guards. Restarting a connection attempt re-arms its new boundary
after an older queued timer fires; link-hint changes also request a host tick.

WASM uses the SDK's existing `after:<milliseconds>` wake string alongside
`immediate`, `deferred` and `after-current-turn`. Callbacks run outside database
borrows. No SDK callback ABI change is required. Registering a scheduler also
arms outstanding deadlines, and timer delays round up to avoid consuming a
sub-millisecond deadline early.

## Wire and storage

No persisted fields, tables, wire tags or payload layouts change. The existing
response is named `jazz.sqlite.sync.read-settlement.v1`, version 1, with the
postcard `SyncMessage::Settled` tag 11 and the existing `id`, `through`, `window`
and `global` fields. `crates/jazz/fixtures/sqlite_read_settlement_v1.json` pins the
local and global response bytes and checks decoding/re-encoding.

Relays may now send a local completion before the global completion. Every hop
must run the matching implementation to rely on this distinction. Older readers
that ignore `global` cannot provide the foreground authority-wait guarantee.
No capability negotiation or compatibility migration for experimental stores is
introduced. The additional deadline and settlement bookkeeping is transient.

## Validation and limits

Public Rust integration tests cover cold and warm owners, empty and nonempty
authority answers, delayed worker attachment, timeout and link-failure fallback,
strict foreground Remote reads, pure settlement upgrades, session permissions,
large-value hydration, one-shot cancellation and host timer re-arming. Chromium
WASM tests exercise the public foreground marker, ordinary wire frames, cached
one-shot reads and the real `after:<milliseconds>` timeout callback.

Foreground writes require the current private owner to acknowledge the same
subject and schema. Before that acknowledgement, after detachment or while a
replacement connection is awaiting admission, standalone, staged and streamed
publication fail with an explicit `Query` error. The later
[private-owner port](sqlite-owner-projection-compatibility.md) supplies pending
write recovery and admission; [owner storage receipts](sqlite-owner-stored-compatibility.md)
provide Local durability after the real flush. Global still requires the
authority's receipt. Storage owners and direct replicas retain their write path.

This note covers foreground reads. The
[browser storage bridge](sqlite-browser-storage-compatibility.md),
[durable relay uploads](sqlite-relay-upload-compatibility.md),
[accepted catalogue identities](sqlite-catalogue-compatibility.md) and
[paired accepted reads](sqlite-paired-reads-compatibility.md) have their own ports
and limits. The [browser quickstart qualification](sqlite-browser-reload-qualification.md)
covers the React chat flow and reloads. The native binding port remains
unpublished, and focused browser checks do not qualify the whole E2EE stack.
