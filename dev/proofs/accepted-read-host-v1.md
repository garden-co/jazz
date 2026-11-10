# Accepted read host ABI v1

`jazz.accepted-read.host.v1` carries a successful accepted transaction read
between the shared Rust Binding and its in-process NAPI/WASM caller. It adds no
storage format or core sync wire message. Authority transport remains
`jazz.sqlite.accepted-read.v1`.

The frame is a four-byte little-endian unsigned JSON-header length, that many
UTF-8 bytes, and the existing binding-codec row-batch bytes. The header contains:

- `format`: exactly `jazz.accepted-read.host.v1`.
- `through`: the authority's `served_through` coverage coordinate.
- `schema`: the frozen schema handle as a lowercase hyphenated UUID.
- `evidence`: one entry for each row in batch traversal order. Each entry has
  `rowId`, `state` and `content`. Both origins contain `transactionId` (the
  existing 32-character lowercase hexadecimal committed transaction identity)
  and `position` (the authority coordinate).

All coordinates are canonical unsigned decimal strings covering the full u64
range. No JavaScript number conversion is permitted. Content has a positive
position no later than state, and state is no later than coverage. Deletion and
restoration may advance state while content continues to identify earlier data.
An empty answer still carries coverage and schema, with an empty evidence list
and the ordinary empty row vector. The header is limited to 16 MiB. Decoders
reject other versions, malformed UTF-8/JSON, invalid identities or coordinates,
duplicate evidence, missing or reordered row pairs, truncation and trailing
bytes. They decode existing row vectors without preallocating from their
untrusted length declarations.

`crates/jazz/fixtures/accepted_read_host_v1.json` independently pins empty and
paired frames, including positions above JavaScript's safe integer range and
u64::MAX. The row bytes are manually assembled from the existing postcard row
contract; committed transaction identities were independently calculated from
main's domain-separated SHA256 contract, using Python `hashlib` independently
of the Rust encoder. The preimage is the 26-byte UTF-8 context
`jazz committed batch id v1`, prefixed by its length as a little-endian u64,
followed by the transaction time as a big-endian u64 and the 16 node bytes.
The first 16 digest bytes form the lowercase hexadecimal identity. State uses
time 2 and sixteen `02` node bytes, yielding
`eb2d19deaa082a76aa19b6b5305e9135`; content uses time 1 and sixteen `01` node
bytes, yielding `1b4ec6169db167137f4dee6f8a76dc43`.

This baseline update changes the fixture's committed identities and their JSON
and frame bytes to match main. It preserves v1 framing, fixed schema and row
bytes, and all coordinate limits. UUIDv5 and E2EE keyed BLAKE2b are unchanged.
Rust compares encoder bytes to the fixtures; TypeScript decodes them and exercises malformed variants.

The host method `acceptedRead(query, id, includeDeleted, timeoutMs, author?,
claims?)` admits synchronously into the existing serializable read queue. Its
bytes or PendingNativeRead use the existing poll/cancel lifetime. `timeoutMs`
must be a nonnegative JavaScript safe integer. It includes core queue time.
Omitted claims use registered core claims. Frozen identity, claims and schema
identify admitted scope; current policies still govern reads and commit.

The optional final `acceptedEvidence` boolean on `beginTransaction` opts in
before batch creation. Absent/false retain ordinary admission. True requires
serializable mode and disallows attribution. An omitted author resolves from
the actual opened Binding peer, including schema views; SYSTEM/backend opens
require an explicit session. The existing explicit-author authentication path
still applies. Invalid capability admission creates no batch. An explicit SDK session registers
its claims once after native identity admission; registration failure abandons
the batch. Accepted queries do not reinstall claims, so later claim changes
remain visible to core validation. This optional
argument is an in-process host ABI addition: it is neither persisted nor sent
as an independent wire capability.

The SDK selects this mode through internal preparation and tracks accepted
reads in the transaction's existing pending-read set. Immediate SDK commit
waits for those reads before invoking core commit. A raw host commit before its
pending read completes retains core consume/refuse semantics. Already-open
ordinary transactions cannot gain accepted capability. New SDKs refuse a host
without `acceptedRead`; old callers continue using the ordinary methods. The
matching Rust and TypeScript artifacts must be shipped together for v1 use.
