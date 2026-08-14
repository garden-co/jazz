# File content adapter receipt

Commit-local scope: `content.file-v1` implements the in-core immutable inline
extent tree and bounded manifest-local byte edits. It is deliberately not an
external object-store or signing implementation.

Implemented and exercised by `file_content` tests:

- domain-scoped, content-addressed leaf/node/root storage;
- fanout-32 immutable tree construction and retained-root reads;
- more than one active edited extent in a single `editTail`;
- overwrite/insert/delete decoding and full-manifest range materialization;
- foreground consolidation to a new root with an empty tail;
- exact historical manifest reads after later edits;
- fail-closed cross-domain reads and malformed operation input;
- same-root, disjoint edit merge, and index values materialized from root plus
  tail rather than root alone.

The edit tail is bounded by the foundation column schema. Consolidation is
foreground-only: no background worker publishes `{newRoot, []}` because the
core still has no expected-manifest CAS that can protect a racing tail edit.

## Explicit external-descriptor boundary

`FileUploadReceipt` defines the data that an eventual authority-issued upload
receipt must bind (domain, digest, length, immutable generation, and key
version). No endpoint issues or verifies such a receipt here; no signed URL,
object-store capability, blob fetch, orphan cleanup, or external descriptor
is persisted. Therefore this change makes **no** claim about upload authority,
HTTP Range behavior over a remote object, or production cleanup.

Before descriptor-backed leaves are enabled, the server implementation needs
an authority-rechecked private conditional upload, receipt verification before
manifest publication, bounded read signing after manifest reachability checks,
and grace-period cleanup of rejected private uploads. Those tests must include
altered digest/generation/domain/range, expired/replayed receipt, guessed
descriptor ID, cross-domain read, corrupt returned range, rejected publication,
and retained-history reachability.
