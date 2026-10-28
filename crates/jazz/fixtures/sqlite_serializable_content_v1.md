# Serializable read content, version 1

A pending counter update can keep its publication number while replay over a
new accepted base changes its value. A UNION result can keep its row IDs,
versions and values while a supporting-row change moves a result to another
semantic arm. Version-only read sets miss both changes.

New serializable reads retain logical content digests alongside their ordinary
row/version read sets. Local commit validation compares both. Relays preserve
the original digests while translating own-write publication positions through
the existing mapping. The authority compares them with a fresh evaluation of
the original query before applying any writes. Empty reads retain their query,
so a new matching row still conflicts. The existing admitted staged-prefix,
permission, conflict-code and text-merge contracts remain in force.

## Content roles

`jazz.sqlite.serializable-read-content.v1` is BLAKE3 derive-key over a postcard
tuple, in this order: table name, fixed 16-byte row UUID, deletion flag, projected
named cells, joined row identities and optional original history identity.
Joined identities are `(fixed row UUID, optional (transaction number, fixed node UUID))`;
the root's optional history identity has the same transaction/node shape. Cell
names and typed values retain their existing postcard field layouts. Numeric
publication aliases are excluded; content and original history identity remain.

A UNION occurrence uses `jazz.sqlite.serializable-union-read-content.v1` over
the postcard tuple `(plain preimage bytes, semantic arm label)`. It therefore
binds duplicate roots to distinct arms without changing the plain-row role.
Narrowed projections include only their projected cells; retained aliases remain
in those named cells and in the original query AST.

`sqlite_serializable_content_v1.json` pins plain and UNION preimages retained
from the independently specified source fixtures. Its complete commit frame is
assembled from explicit fields. The ordinary Query body is copied from the
pinned v4 Subscribe fixture; all new-kind fields are specified independently.
The integration test compares these digests with a real pending upload.

## Wire and pending storage

`jazz.sqlite.serializable-content.v1` is append-only WireKind tag 2, named
`SerializableContentV1`. Its fields are snapshot, reads, own publication
positions, then a nested digest vector: one vector per read, one fixed 32-byte
digest per row. Reads and own positions retain the existing ordinary layouts.
The envelope remains `jazz.sqlite.sync.v4`; no existing message or kind changes
its bytes. Write-only transactions retain the ordinary Serializable kind.

Both relays and authorities reject missing or extra digest entries. Retained
relation queries require the new kind, including empty results, so dropping the
content vector cannot downgrade their occurrence validation. Historical plain
Serializable messages remain readable with their original version-based
contract; all newly produced reads use the content-bearing kind.

Pending uploads store the same Commit body, without the network envelope, in
`_jazz_pending.commit_msg`. Reopening retains the content evidence. No SQL table,
row, history, chunk or ordinary own-dependency layout changes. Historical
fixtures remain unchanged. Older binaries cannot decode the new kind, including
from pending storage, and must not reopen a store containing it. There is no
negotiated fallback: clients, relays and authorities using these commits need
matching support. Catalogue and future accepted-read semantic envelopes remain
separate compatibility decisions.
