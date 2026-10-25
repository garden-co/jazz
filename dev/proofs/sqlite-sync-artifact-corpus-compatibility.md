# SQLite sync artifact corpus

The fixture role `jazz.sqlite.sync-artifact-corpus.v1`, version 1, inventories the
current SQLite semantic messages. This is a test corpus version, not a new wire
format. It changes no enum tags, field order, serializer, feature bits, protocol
version, durable storage or permission decisions.

The corpus contains 58 samples spanning all 41 `SyncMessage` variants. Thirty-eight
samples refer to unchanged bytes in 19 existing fixtures; their source hashes
prevent silent replacement. Twenty base samples were assembled independently
from the field layouts. Typed Rust constructors check those new pins. The pins
include every SQL cell arm, populated root and branch images, a 64 KiB opaque
chunk field, settlement windows, refusals, fates and permission advice. The
opaque chunk sample exercises encoding size, not chunk-hash validity or storage.

The Rust decoder test matches every variant exhaustively and checks complete,
canonical input. The artifact test invokes the same production SQLite decoder
through small `__testValidateSqliteSyncFrameCorpus` exports in NAPI and WASM.
These exports decode and discard a message; they create no connection and grant
no authority. The NAPI export is omitted from production TypeScript declarations,
like the existing corpus bridge. Each sample must decode, while an appended byte,
truncation and an overlong enum tag must fail.

Shared v6 control coverage remains: seven Hello frames, two structured errors,
and eleven malformed/control/feature/compression rejections. NAPI must still
advertise zstd, and WASM must still export its actual feature mask. The separate
structured-error test is unchanged.

SQLite sends raw semantic messages through the host transport; the old Groove
ordered-channel semantic payloads are not that protocol. All 25 frozen legacy
semantic batches remain as explicit incompatibility checks, both at the legacy
artifact validator and as physical frames at the SQLite decoder. Their bytes are
unchanged. A legacy rejection does not establish which nested guard rejected it:
semantic invalidity can precede a compression or feature check. Separate
WebSocket and negotiated-session tests cover those guards directly.

The mapping below records what moved and what the corpus actually establishes.
Successful decoding alone never proves authorization, deployment, settlement or
query correctness.

| Legacy fixture                                                | Current representation and coverage limit                                                                                                                                  |
| ------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `chunk_upload_start_root_descriptor`                          | `PushChunks` and `ChunkRequests`; the old upload-start descriptor is retired. This corpus pins chunk envelopes, not descriptor validation.                                 |
| `chunk_upload_nodes_requested_root`                           | `PushChunks` with a 64 KiB opaque field and `Chunks` with missing references; actual large-value synchronization has separate runtime tests.                               |
| `session_claims_role_editor`                                  | Claims enter through authenticated connection admission or explicit trusted scope fields. There is no ambient session-claims message; parsing a scope grants no authority. |
| `fate_update_accepted_global`                                 | `Fate`, all four `FateV1` result arms and local/global `Settled`; runtime authority determines the meaning of receipts.                                                    |
| `register_shape_todos`                                        | Queries travel directly in `Subscribe` and `Fetch`; the separate shape-registration message is retired.                                                                    |
| `subscribe_empty_todos_binding`                               | The current ordinary `Subscribe` fixture also exercises a populated binding and resume cursor.                                                                             |
| `subscribe_fast_known_state_authorization_progress`           | `Hold`, `Subscribe.since`, `Origins`, `Settled` and accepted evidence have separate pins. The old authorization-progress token is retired.                                 |
| `subscribe_delegated_session_claim_snapshot`                  | Trusted `ScopedFetchV1` and scoped accepted/catalogue requests carry frozen claims. This is not a claim that the old delegated-subscribe message remains supported.        |
| `authorization_scope_intent_delegated_session_claim_snapshot` | Scoped fetch/evidence and accepted overlay messages carry explicit scope; the old intent/receipt encoding is retired. Trusted-link enforcement has separate runtime tests. |
| `unsubscribe_todos_binding`                                   | `Unsubscribe`, including a multi-byte request ID.                                                                                                                          |
| `subscribe_rejected_unsupported_shape`                        | `Refused` with the current query error code and a reason.                                                                                                                  |
| `subscribe_rejected_server_table_not_found`                   | A separate `Refused` sample with the current query error code and missing-table reason.                                                                                    |
| `view_update_reset_with_covered_input`                        | `Rows`, `Origins`, `AcceptedRowsV1` and settlement/answer messages pin the constituent encodings. They do not establish delivery ordering.                                 |
| `view_update_mixed_version_carrier_runs`                      | A populated `Rows` sample contains root and branch images; the old carrier-run grouping is retired.                                                                        |
| `view_update_supporting_row_branch`                           | Branch images and `Hold`/`Evict`/`Unlist` keys, plus readable-policy subscriptions and scoped evidence, have current pins.                                                 |
| `commit_unit_mergeable_empty`                                 | Empty mergeable `Commit` plus unchanged fixtures for the three published serializable kinds. The pending union kind is outside this corpus.                                |
| `publish_schema_todos_body`                                   | Schema publication uses the admin/deployment path; `Schema` identifies an admitted version on sync. The corpus does not qualify deployment.                                |
| `publish_lens_todos_body_identity`                            | Lens publication uses the admin/deployment path. No legacy publication message is restored.                                                                                |
| `publish_schema_with_lens_todos_body`                         | Combined deployment belongs to the admin/deployment path, not semantic sync admission.                                                                                     |
| `catalogue_ack_schema_applied`                                | `CatalogueCoveredV1` answers a catalogue-read request. It is not a schema-deployment acknowledgement.                                                                      |
| `catalogue_snapshot_todos_lineage`                            | Current catalogue metadata, fetch and coverage fixtures pin SQLite's representation. They do not qualify every migration.                                                  |
| `view_update_physical_delta`                                  | Current row images, evictions and unlisting replace the old physical-delta carrier. Runtime behavior is tested separately.                                                 |
| `view_update_current_becomes_historical`                      | Root/branch images and retained-content witnesses pin the current fields. The transition itself requires runtime history tests.                                            |
| `view_update_part_non_final`                                  | The old view-part envelope is retired; SQLite rows and settlement use separate messages. This corpus does not establish fragmented-delivery correctness.                   |
| `catalogue_snapshot_multiple_predecessors`                    | The legacy bytes must be refused. Current catalogue fixture decoding does not qualify multiple-predecessor migration support.                                              |

This port deliberately does not restore compatibility with unreleased Groove or
intermediate SQLite encodings. If an independent byte pin or legacy refusal
fails, investigate the encoder/decoder and contract before changing the pin.
