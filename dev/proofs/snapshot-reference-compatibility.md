# Application snapshot references, version 1

Applications persist a snapshot as one ASCII string, prefixed by
`jazz-snapshot-v1:` followed by lower-case hexadecimal bytes. The prefix names
and versions this application format. Unknown versions, invalid hexadecimal,
truncated fields and trailing bytes are refused.

The decoded bytes are:

| Offset | Field            | Encoding                                                                        |
| ------ | ---------------- | ------------------------------------------------------------------------------- |
| 0      | Owner node       | 16 UUID bytes in UUID/network order                                             |
| 16     | Global base      | Unsigned 64-bit little-endian integer                                           |
| 24     | Local base       | Unsigned 64-bit little-endian integer                                           |
| 32     | Dot count        | Unsigned 32-bit little-endian integer                                           |
| 36     | Transaction dots | Count repetitions of an 8-byte little-endian transaction time and 16 UUID bytes |

Rust writers sort dots by transaction time, then node UUID, and remove duplicates.
Readers accept any dot order and duplicate dots. Re-encoding a parsed reference
in Rust therefore canonicalises its string; TypeScript preserves the opaque
input string. Both forms describe the same frontier. The shared fixture pins
this behaviour as well as canonical round trips. The reference describes the
existing dotted snapshot frontier; it contains no rows, credentials or history
retention lease.
Capture adds explicit dots for known unsequenced transactions so later rejection
or missing pending history is observable when restoring the reference. Capture
enumerates the unsequenced index bucket, without scanning settled history.

`crates/jazz/fixtures/snapshot_ref_v1.json` pins independently authored application
strings, u64 extrema, transaction dots and invalid envelopes. Rust and TypeScript
consume the same fixture. TypeScript treats the coordinates as opaque bytes.

## Existing storage and sync compatibility

The binary serde layout of `SnapshotRef` remains exactly owner, global base,
local base and dots in that order. Storage and sync continue to carry that
established representation. The public application string is decoded at the
human-readable binding boundary; it does not replace the binary representation.
The compatibility test compares the binary bytes with the preceding field
layout and reads them through the current decoder.

Existing human-readable field objects remain accepted by internal bindings.
The supported application API accepts a typed reference obtained from capture
or `parseSnapshotRef`. Applications store `serializeSnapshotRef(reference)` in
a string column, JSON value, local storage or another string-preserving store.

## Native foreground command compatibility

Native relay ABI 2 appends `CaptureSnapshot` at command ordinal 34. Existing
command and response ordinals are unchanged. Capture returns the existing
`Rows` response, whose bytes are the UTF-8 application reference, or the normal
pending-operation response. Both paths use the ordinary foreground owner queue.
The ABI stays at 2. The linked Rust artifact advertises capture support through
the additive `jazz_native_relay_supports_snapshot_capture` C function, exposed
as the optional `supportsSnapshotCapture` property on the native JSI handle.
Older native handles omit this property and continue to support existing
operations. Only capture is refused, with `UnsupportedSnapshotError`
(`code: "unsupported_snapshot"`), before sending the new command. Rebuilding
the native app is required only to use capture. This change adds no server wire
feature or storage epoch.

## Availability and permissions

Restoring an encoding does not establish history coverage. Local frozen-history
resolution refuses an unknown global cut, missing or rejected transaction dots,
partial transaction payloads and foreign partial stores that cannot certify the cut.
The originating store can reconstruct its own captured local view after server
receipts advance its known global cut; a nonzero cut does not require that
client to hold the authority's complete history.
Frozen source authorization preserves the source's declared delivery metadata,
including version and coverage witnesses, for ordinary server subscriptions.
A restarted local-only store can read its pending frontier when the complete
captured transaction dots remain available. An authority-covered query uses the
standard sync source path. History truncation remains refused at store open.

An open frozen subscription retains the base values materialised when it opens.
Later base writes and transaction fate changes do not rewrite those values.
Its draft head remains an ordinary live maintained source. The reference does
not retain durable history: a new or reopened read reports unavailable history
if the captured base can no longer be reconstructed, including after a captured
pending write's rejected history has been removed. Existing subscriptions can
keep their materialised base even when a new read cannot reconstruct it.

The reference grants no read or write authority. Frozen base rows, relations,
policy dependencies and copy-on-write mutations retain ordinary branch-view
permission checks. No unavailable snapshot is reinterpreted as a live base.
