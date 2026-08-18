# Logical messages and physical transport frames

## Decision

Jazz sync operations are logical messages. The link transports those messages
as bounded physical frames. A query result, catalogue publication, commit, or
repair response is not required to fit in one frame and semantic layers must
not split otherwise-atomic operations merely to satisfy a link frame budget.

The sender encodes one `SyncMessage`, applies negotiated per-message
compression, and then either emits the ordinary message envelope or fragments
that encoded payload. Every fragment carries the negotiated/session metadata,
the connection-direction monotone logical-message id, the BLAKE3 integrity
digest of the complete encoded payload, the exact total length, the byte
offset, and fragment bytes.
Fragments are idempotent: an exact duplicate is ignored and a conflicting
duplicate rejects the logical message. Delivery may be reordered within a
logical message. Complete logical messages remain ordered on transports whose
semantic stream is ordered. Per-message compression keeps reordered assembly
independent and permits a hard decompressed-output cap before semantic decode.

The receiver validates the frame and authenticated session metadata before it
admits fragment state. It accepts only in-range, non-overlapping extents, checks
exact contiguous coverage and the full digest, then decompresses and decodes
the semantic message. No partial semantic message reaches `Db`.

Reconnect creates a new adapter/codec epoch and discards incomplete assembly.
Known-state replay remains the retry mechanism; fragments themselves do not
outlive a connection. Implementations bound physical frames, concurrent
incomplete messages, aggregate staged bytes, advertised logical length, and
recent-completion deduplication. These are configurable/resource-defense
budgets, not query semantics and not a 2 MiB logical-message ceiling.

## Limit inventory

Transport-only limits to remove:

- `MAX_SYNC_MESSAGE_BYTES` and `MAX_COMMIT_UNIT_BYTES`;
- peer-side `RowVersionPayloads` splitting by encoded message size;
- view-update splitting and result-parent rejection solely to fit
  `MAX_WIRE_FRAME_BYTES`;
- transport-driven row limits or explicit unbounded declarations for array
  subqueries.

Limits retained for semantic or adversarial-resource reasons:

- `MAX_WIRE_FRAME_BYTES`: allocation bound before frame decode;
- `MAX_SHAPE_AST_BYTES`: executable query/shape complexity admission;
- commit-version, repair-ref, branch-key-qualified repair, known-state-ref and structured
  result depth/width limits: CPU/fan-out/state bounds independent of framing;
- receiver in-flight/aggregate/advertised-length budgets and WebSocket frame
  limits: unauthenticated allocation and peer-fairness defenses.

Array subqueries now have ordinary SQL-like semantics: omitted `limit` means
unbounded; a finite `limit`, including zero, is optional query semantics.
Transport fragmentation carries large parent replacements atomically.
