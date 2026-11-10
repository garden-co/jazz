# Candidate permission sources v1

`ExistsIncludingCreated` extends positive INSERT checks with independently
authorised root inserts from the same serializable commit. Each new witness
must pass its own checks without an unsupported cycle, then pass again against
the final state. Ordinary EXISTS occurrences in such a unit still use accepted
evidence. Marking one occurrence does not mark nested or sibling occurrences.
Mergeable and callback transactions cannot promote new rows to witnesses.
Schemas without this capability keep the existing ordinary transaction executor.

The public policy JSON adds the named `ExistsIncludingCreated` variant with
`table` and `condition` fields. The lowered `JoinVia` encoding keeps its original
eight fields. The `target` field retains tags 0, 1 and 2 for ordinary Column,
RowId and Uncorrelated sources. It appends tag 3 (`AuthorizedCreatedColumnV1`)
and tag 4 (`AuthorizedCreatedRowIdV1`). There is no created Uncorrelated target.
Old readers reject the new tags instead of silently treating them as ordinary
sources. The canonical join key adds bytes `61 01` before the filter fields only
for the new capability. Model tests pin JSON, postcard bytes, canonical keys,
legacy decoding, truncation and unsupported tags using independent constants.

The server catalogue policy grammar appends tag 22 for `ExistsIncludingCreated`,
followed by the existing u32 little-endian table-name length, UTF-8 table bytes
and a child policy expression. Existing tags and bundle versions remain unchanged;
old decoders reject tag 22. A separate server fixture pins these bytes and tests
truncation, malformed UTF-8 and unknown tags. The TypeScript builder and schema
normaliser preserve the occurrence marker while encoding nested literal values.

The JSQL v4 envelope and all ordinary message and query bodies stay unchanged.
Existing policy variants keep their encodings. A schema using the new capability
requires supporting runtimes at every endpoint that admits or evaluates it;
there is no downgrade or rolling-upgrade bridge for that schema.

`jazz.sqlite.uuid-array-index.v1` is a derived storage role for declared leading
UUID-array indexes. Its physical name includes the byte lengths and stored table
and column names. Keys contain a raw 16-byte member UUID, a current/history kind
(0/1), the raw row UUID, branch bytes and the image sequence. Its DDL is pinned
in `sqlite_uuid_array_index_v1.sql`. Physical tests independently assert raw keys,
indexed lookup, history movement, deletion, rollback and one-time backfill.
Installation and backfill run inside a savepoint; subsequent writes maintain the
index through triggers in the same row transaction. No existing row, history,
chunk or manifest encoding changes. Older runtimes do not use this derived role.

Proof scratch tables are connection-local TEMP tables and carry no durable or
wire contract. Dependency walks and SQL instruction work have explicit limits;
unsupported correlations fail with typed errors instead of scanning unrelated
rows. New witnesses are restricted to root, singly written coordinates absent
from accepted history. Branch moves, created array sources, inherited creation
capabilities and provisional reads needing large-value preprocessing remain
explicitly unsupported.
