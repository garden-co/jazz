# Transactions

## 11. Transactions

Every transaction has:

- public transaction id
- physical transaction id
- writer node
- node-local epoch
- optional global epoch assigned by authority
- conflict mode
- transaction kind
- outcome
- durability receipts/frontier
- creation time
- typed metadata containing write facts and persisted observed facts

Transaction kinds include:

- data
- branch metadata
- schema metadata
- permission metadata

Outcome values are:

```text
pending
awaiting_deps
accepted
rejected
```

Durability/acceptance receipts track where a transaction has become replayable:

```text
local
edge
global
```

For v0, the hot transaction row may store the current outcome and global epoch
mutably. Rejection details should live in a side table keyed by transaction id
or physical transaction id, not as a wide field on the hot transaction row.
The Rust prototype currently represents `awaiting_deps` as a side-table marker
while leaving the hot outcome `pending`; this keeps outcome ordering simple
while still making dependency waits durable and queryable.
That marker must persist the auth user context that the authority used for the
original validation attempt. Dependency arrival must not require the client to
resend the original write bundle.

The local write path:

1. allocates transaction id and local epoch
2. begins an embedded database transaction
3. records the transaction
4. appends all history rows
5. records write facts and persisted observed facts
6. updates or invalidates current projections
7. commits the embedded database transaction
8. publishes local subscription diffs

Transaction isolation:

- A transaction reads from a consistent semantic snapshot of the whole database
  captured when the transaction starts.
- Reads inside the transaction include the transaction's own staged writes,
  layered over the start snapshot.
- Reads inside the transaction do not include staged writes from any other
  transaction.
- Writes committed by other transactions after the transaction starts are not
  visible inside the transaction; starting a new transaction is the way to read
  a later current view.
- Sealing publishes all staged mutations as one transaction. The implementation
  may lower staged state through SQLite transactions, in-memory overlays, or
  another embedded-database mechanism, but the public semantics are the
  start-snapshot plus own-staged-writes view.

Patch updates preserve omitted fields from the effective visible row. The
effective base may be a current branch row, a row inherited from branch sources,
or a pinned historical base snapshot. Unknown user fields fail closed before
history/projection writes; they are not silently dropped.

Authority acceptance enriches the existing transaction. It must not create a new
public transaction id.

Authority rejection keeps the transaction and history rows. Visibility and
projection repair make rejected versions disappear from ordinary reads.

An edge that cannot validate a mergeable transaction because required
policy-influencing facts are missing should mark it `awaiting_deps`, request or
subscribe to the missing facts, and re-evaluate after they arrive.
`awaiting_deps` is not acceptance and must not make an authority-accepted
version visible. Globally consistent exclusive transactions do not use
`awaiting_deps` for ordinary policy-dependency cache misses; they are forwarded
to the global authority and must always receive final `accepted` or `rejected`
fate there.

An `awaiting_deps` transaction keeps its public transaction id and history rows.
It is not reported as rejected, and ordinary current projections must exclude
its row versions. When the missing facts arrive, the same sealed transaction is
re-evaluated. If validation succeeds, the edge clears the awaiting marker and
accepts/receipts the original mergeable transaction. If validation fails for a
reason other than missing dependencies, including a dependency row arriving but
still not being readable by the transaction's auth user, the edge rejects the
original mergeable transaction.

When a previously missing policy dependency becomes visible, the authority
should repair any stored observed-read facts that were recorded as missing so
future sync exports describe the dependency version that allowed validation to
complete. This repair enriches the read set for the same transaction; it does
not create a replacement transaction.

Edge acceptance of mergeable transactions sets the transaction outcome to
accepted and records an edge receipt without a global epoch. Global acceptance
later records the global epoch and global receipt on the same transaction.

Multiple transactions may share one global epoch. A global epoch is an authority
batch/order point, not a unique transaction coordinate. Deterministic ordering
within one global epoch uses a stable tie-breaker such as physical transaction
number or public transaction id, depending on the storage context.

Waiting semantics:

- waiting on a mergeable transaction may target local, edge, or global
  durability
- waiting on an exclusive transaction with any tier other than global is a
  runtime error
- waiting on an exclusive transaction at global resolves only after global
  acceptance or rejects if the authority rejects it

Open issues:

- exact durability receipt layout
- explicit fate partial order and merge rules for all incoming-sync cases
- timeout/retry behavior for transactions that remain `awaiting_deps`
- dependency request/subscription protocol for proactively fetching missing
  policy facts
- forwarded exclusive transaction retry, offline storage, auth-context
  preservation, and global fate propagation
- audit-grade append-only fate/receipt history
