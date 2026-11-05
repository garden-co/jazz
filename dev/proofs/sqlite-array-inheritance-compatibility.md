# SQLite UUID-array permission inheritance

Forward inheritance checks the target rows named by a UUID array. Reverse
inheritance finds source rows whose UUID array contains the root ID. Both use
existential membership: duplicates do not duplicate grants, and empty, missing,
null or denied targets grant nothing. The inherited operation and depth limit
remain unchanged. Inherited UPDATE still checks the target's USING capability.

Candidate authorization retains accepted-only inheritance and final-state
revalidation. An independently insertable grant cannot authorize a dependent
write in its own unit. Revoking an accepted grant invalidates the dependent
write, and rejection rolls back rows and derived membership together. Accepted
reads select the exact current or historical image at their authority cut.
Trusted scoped reads carry the same private permission closure and immutable
claims as scalar inheritance. Ordinary connections gain no delegation ability.

## Durable role and installation

The new derived role is `jazz.sqlite.uuid-array-index.v1`. Each indexed UUID-array
column has a table named `_jazz_array_members_v1_<table-byte-length>:<table>_<column-byte-length>:<column>`.
Names use physical stored names and UTF-8 byte lengths, so schema renames retain
the same index. The table is WITHOUT ROWID, keyed by `(member, kind, id, branch,
seq)`. UUID members and IDs are raw 16-byte blobs, branch is the existing exact
branch-key blob, and sequence is the existing positive SQLite integer. Kind 0
means current; kind 1 means historical. Duplicate members collapse to one key.
The row index `(kind, id, branch, seq)` bounds deletion of one image's members.
The complete DDL is pinned byte for byte in
`crates/jazz/fixtures/sqlite_uuid_array_index_v1.sql`; tests also pin raw key bytes.

Installation occurs after physical schema creation/adoption or view admission.
A savepoint creates each derived table, its index and six triggers, and backfills
that column's current and historical images. The completed table is the install
marker. A failed installation rolls back its table, triggers and backfill;
reopening a completed installation does not scan the application rows again.

This one-time index build reads all existing versions of that indexed column.
It adds storage proportional to their distinct UUID memberships and can increase
first-open time for an existing large store. It is a schema/index migration, not
a fallback query plan. Row writes maintain memberships through SQLite triggers,
including replay, update, deletion, restore and rollback. Tombstones retain
version membership, while the permission query continues to exclude deleted
images. Compaction removes memberships when it removes their historical rows.

Candidate reverse lookups use the leading member key. Before-state history is
driven by the unit's publication-sequence index; arbitrary accepted cuts use
member-indexed history and select the newest eligible version. Forward evidence
looks up only IDs present in the root array, with duplicate IDs removed. Existing
evidence budgets still bound dependencies; there is no whole-table fallback.
The pre-existing ordinary reverse-array correlation is unchanged; this proposal
does not claim a performance improvement for that path.

No application row, catalogue, sync receipt or wire encoding changes. Earlier
unreleased SQLite builds lack these permission lookups; deployment still needs
coordinated upgrades. No compatibility with intermediate unreleased releases is
promised. The index is derived from existing row bytes and does not grant a new
authority capability. Branched and candidate-created array evidence remain
explicitly unsupported. Query-array output and remote union coverage are outside
this change.

## Qualification

The proposal adds public API tests for direct/relay isolation, private evidence,
historical reads and stale commits, forged delegation, reopen, delete/restore,
new-grant exclusion and atomic revocation. Internal format tests are needed to
inspect durable key bytes, leading-index plans and partial-install rollback.
The existing candidate test that expected array inheritance to be unsupported
is changed to require success from its accepted owner. The existing SDK named
relation tests stay unchanged and must pass after the port.

This file accompanies an unapplied review proposal. None of its new runtime or
tests has compiled or run. Qualification must include the existing candidate,
scoped-evidence and array suites, both unchanged named-relation SDK tests,
sealed artifact production and the required local lint partition before push.
