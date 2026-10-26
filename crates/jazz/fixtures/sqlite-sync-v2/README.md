# SQLite sync v2 and storage format 2

The SQLite/E2EE reconciliation uses **jazz.sqlite.sync.v2**. Each semantic
frame starts with the five literal bytes `4a 53 51 4c 02` (`JSQL`, version 2),
followed by one canonical Postcard message. The existing websocket Hello and
Error carrier layouts remain unchanged. A semantic decoder rejects a missing
header, another codec version, noncanonical integers and trailing bytes before
queueing a message. It does not infer a codec from an enum tag.

This version combines upstream's read tiers, per-operation timestamps, echo
acknowledgements and multipart row frames with E2EE attribution, accepted
history, catalogue admission and scoped permission evidence. Upstream tags
0–20 keep their order. E2EE extensions occupy tags 21–44; tag 45 is
`OriginFateV2`, carrying the original writer node, transaction, authority cut,
result, echo hints and Local/Global flag. A Local receipt cannot retire a
private foreground's original upload or establish accepted history. Ordinary
foregrounds may clear their local send queue after a Local receipt while the
write remains pending; their durable worker still owns the authority upload.
UNION coverage describes the hydrated
prefix; readers validate its scope/evidence before applying the requested
outer page. Serializable validation retains the original full query.

`frames.json` was assembled independently from field order, fixed arrays,
UTF-8 string lengths, unsigned LEB128 integers, and Option/Result tags. It pins
the version fence, the colliding tags, per-operation timestamps and original
writer receipts. Historical v1 fixtures remain unchanged in their original
files. Changes to this contract require another version and new fixtures.

The shared `../sqlite_sync_wire_v2.json` corpus covers all 46 message tags,
21 extended forms and malformed frames. Upstream tags 0–19 keep their pinned
body bytes and gain the v2 prefix; the E2EE cases use the independent fixtures
in this directory. Rust and the WASM/NAPI artifact tests use the same corpus.
The artifact tests also retain both historical SQLite corpora and Groove
semantic frames as rejection cases. The carrier Hello/Error format stays v6.

The storage stamp is **2**. Its pending commits contain the v2 message layout,
including per-operation timestamps and echo hints. The relay journal uses
`JRU\x02` (`jazz.sqlite.relay-upload.v2`) before its nested commit. The SQL
journal table name retains its original suffix; the file stamp and journal
prefix admit the nested bytes. Unstamped stores and stores stamped 1 are
refused before SQLite journal configuration or schema writes. This code does
not delete or reset them. There is no migration or backward reader for either
unreleased branch's old pending-upload layout.

All connected runtimes must therefore use matching v2 artifacts in the same
release: server, browser/WASM, Node/NAPI and React Native. A mixed deployment
fails codec admission. Matching codec bytes do not confer trusted-host,
identity, private-owner or permission authority; those checks still apply.

Row frames remain below the carrier's frame ceiling. Reassembly has both the
logical-message byte ceiling and the existing frame-count ceiling, drops
partial buffers on error and delivers nothing until the final Rows frame.
