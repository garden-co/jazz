# Authority accepted evidence wire contract

`jazz.sqlite.accepted-read.v1` extends the existing sync `Read` route. Ordinary read, row, catalogue and transaction bytes retain their existing encodings. This extension creates no durable storage format.

An accepted read uses `ReadSpan::AcceptedOnce` (discriminant 2, after Live and Once). Its fields are optional authority coordinate, fixed 16-byte schema version and optional identity/claims scope. The outer Read must use Remote and Current, with no cache resume coordinate. Its existing wait field bounds relay waiting in milliseconds. A relay freezes the authenticated effective scope before forwarding; only a SYSTEM connection can delegate another scope. An ordinary connection's identity and explicit claims take precedence over the request.

The response is `SyncMessage::Accepted` (discriminant 20, after ConnectionSettled). Fields, in order:

1. Request id and authority `served_through` coordinate, unsigned LEB128 integers.
2. Schema version, exactly 16 bytes without a length prefix.
3. Scope: length-prefixed canonical author UTF-8 string, then the existing claims map encoding.
4. Length-prefixed evidence sequence. Each entry contains a row UUID, state version and content version. Each UUID uses the existing UUID byte-string representation (length 16 followed by 16 bytes). Each version contains transaction time, writer-node UUID, then authority sequence. Integer fields use unsigned LEB128.
5. Length-prefixed ordinary encoded-row frame. The frame's existing descriptor and packed-record contract is unchanged.

Evidence follows result order. Row IDs and counts must match the decoded rows, with no duplicate IDs. Both origins are accepted, and `0 < content.sequence <= state.sequence <= through`. The response schema and an explicitly requested coordinate must match the request, including empty results. This result is evidence for that evaluation, not a write receipt or reusable authorization decision. A serializable consumer must still validate its read set before mutation.

Relays forward the authority's result through the existing one-shot request lifetime. They neither reconstruct evidence from cached rows nor downgrade a failed request to local data. Requests are cancelled through ordinary Unsubscribe and subscriber cleanup. Deadline expiry returns NotObserved. The accepted shape remains unbranched stored rows; unsupported aggregates, joins, nested results and history queries are refused.

The independent `crates/jazz/fixtures/accepted_read_v1.json` fixtures pin request, empty response and distinct state/content origins. They were assembled from named fields and explicit tags, not by serialising production objects. The nonempty row frame deliberately has a zero-column projection so the provenance fields can be checked independently of application cell types.

Deploy matching extension-capable clients, relays and authorities before using this mode. Older peers cannot decode the appended variant and fail the request/link; there is no silent evidence fallback or compatibility promise for unpublished experimental encodings. Existing ordinary traffic between compatible peers is unchanged. Catalogue freshness and owner-local observations are separate capabilities and do not reuse this result as authority for a later operation.
