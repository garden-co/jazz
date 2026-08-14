# 12. Ordinary-row text documents

## 12.1 Contract

Collaborative text is a library data structure built from ordinary Jazz tables
and transactions. It is not a scalar type understood by the storage engine,
wire protocol, sync path, or authority.

A committed text version names a complete immutable snapshot:

```text
TextVersion {
  document
  base_root
  patches[]       // bounded, ordered inserts against base_root
  length
  previous_version? // lineage metadata only
}
```

Reading a version MUST load only that version's base root and bounded patch
frontier. `previous_version` MUST NOT be followed to reconstruct content. This
keeps historical read work independent of Jazz history depth.

The document's current-version pointer and the new immutable version MUST be
written in one ordinary Jazz transaction. Consequently text edits participate
in the same durability, sync, authorization, branch, and history boundaries as
other row writes.

## 12.2 Bounded frontier

Small inserts append to the version's patch frontier. The library MUST bound
both patch count and encoded patch bytes. Crossing either bound synchronously
materializes a new immutable rope root in the same transaction as the new
version and document-pointer update. No application batching is required: one
Unicode-safe insertion may be one durable Jazz transaction.

Rope nodes are immutable ordinary rows. Leaves contain bounded UTF-8 text;
branch nodes contain child references and subtree code-point lengths. A root is
a complete snapshot, not a checkpoint that requires ancestor replay.

## 12.3 Initial public scope

The first API supports create, current read, exact-version read, and insertion
at a Unicode code-point offset. It deliberately does not claim automatic
semantic merging of concurrent document heads. Competing document-pointer
writes use ordinary Jazz merge behavior, while both immutable versions remain
in history for a future format-aware merge layer.

Applications include the exported three-table definitions in their schema and
define ordinary permissions for documents, versions, and nodes. The library
does not add a privileged ownership or content protocol.

## 12.4 Required evidence

- inserts before, inside, and after non-ASCII text never split a scalar value;
- threshold crossings keep patch count and bytes bounded;
- sampled old versions remain readable after later consolidation;
- deleting or corrupting lineage metadata cannot be necessary for a read;
- a planted missing base node makes the read fail rather than fall back to
  scanning prior versions;
- representative durable edit, materialization, row-count, logical-byte, and
  disk receipts remain reproducible beside the implementation.

## 12.5 Open questions

- automatic merge policy for rare competing heads (plain-text three-way merge
  versus format-aware Markdown/HTML adapters);
- transaction-created child authorization for least-privilege node insertion;
- batched reachable-node hydration and content-addressed subtree reuse;
- deletion and replacement operations beyond the initial insertion slice.
