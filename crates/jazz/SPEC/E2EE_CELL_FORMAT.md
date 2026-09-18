# Encrypted cell storage, version 1

Implementation companion to [chapter 20](20_e2ee.md) and the
[cryptographic formats](E2EE_CRYPTO_FORMAT.md). This E8 contract specifies the
cell format; it does not establish release qualification or an independent
security audit. Those requirements are tracked in [#3125](https://github.com/garden-co/jazz/issues/3125).

## Stored value

An encrypted column stores one non-null BYTEA value, including when its
logical value is null. The common envelope uses mechanism
`jazz.e2ee.cell-record`, version 1. Its payload is:

1. The epoch UUID as exactly 36 lowercase ASCII bytes, including hyphens.
2. The complete CellCipher envelope, including its mechanism ID and version.

The outer envelope selects the record format; it is not an authentication
claim. The reader validates both envelopes and authenticates the epoch in the
operation context. Unknown versions, malformed UUIDs and truncated values fail.
Changing the epoch cannot make a ciphertext valid under a different key.

For the epoch `00000000-0000-0000-0000-000000000001`, the routing bytes are
`30303030303030302d303030302d303030302d303030302d303030303030303030303031`.
The record header is
`4a45324501156a617a7a2e653265652e63656c6c2d7265636f726400000001`.
These precede the inner envelope without another length or terminator.
`packages/jazz-tools/src/e2ee/encrypted-cell-format.test.ts` checks the complete
stored bytes through ordinary writes and reads on WASM and native Node. It uses
a fixed BYOC envelope, literal record headers and the accepted epoch UUID; it
does not call the private cell encoder. This qualifies common framing, not the
cryptographic strength of the deliberately non-cryptographic fixture adapter.

## Authenticated context

The CellCipher receives two fields framed by the existing u32be-length
transcript encoding, in this order:

1. Canonical context version 1, with application `[registry, environment]`
   encoded as the established JSON string pair; policy
   `jazz.e2ee.cell-record.v1`; stable scope-table UUID; space identifier;
   stable data-table UUID; row UUID; stable column UUID; and epoch UUID.
   The recipient field is empty. The decrypting account is not part of
   the application identity.
2. The UTF-8 logical descriptor produced by `encodeCellType`, not the Groove
   storage type: Text, Json and string Enum share a storage carrier but must
   remain distinct authenticated types.

The descriptor is a JSON object with `column_type` and `nullable`. Its type
tag is the logical ColumnType name. Array includes its `element`; Row includes
ordered `columns`; Enum includes ordered `variants`; EnumPayload includes
ordered `cases`, each with a name and ordered fields. Nested fields retain
their names, logical types and nullability. Json includes its schema when
specified. Top-level names, defaults, references, merge strategies and physical
sparse metadata are excluded. A compatible top-level rename therefore leaves
the descriptor unchanged.

Object keys are sorted by JavaScript UTF-16 code-unit order. Arrays retain
their order. Strings and finite numbers use ECMAScript JSON.stringify spelling;
there is no whitespace. Unsupported or non-finite metadata fails rather than
being coerced. This is a pinned version-1 encoding, not a dependency on future
schema-serializer changes. Incompatible changes require a new cell-record
version and corpus review.

For a required text column the exact UTF-8 bytes spell
`{"column_type":{"type":"Text"},"nullable":false}`. Literal fixtures in
`packages/jazz-tools/src/e2ee/cell-type.test.ts` cover text, nullable text, JSON,
enums, bytes, arrays, payload enums and top-level rename/default independence.

## Encrypted plaintext

The plaintext is a Groove packed record containing one logical column named
`value`, encoded with `encodeNativeRowValues`. Decoding uses the same logical
descriptor and `decodeNativeRowValues`, not the physical BYTEA descriptor.
This retains the existing scalar, nullable, array and structured-value byte
representations instead of introducing JSON conversion for dates, bytes or
large integers. Adapters do not choose the value representation.

The public cell-format fixture pins `synthetic` to
`0273796e746865746963` and bytes `de ad` to `02dead`, matching the existing
native row corpus. It checks these plaintext bytes alongside the complete
stored record and the two-part authenticated transcript. Runtime-generated
UUIDs remain variable: the scope, space, row and epoch are checked against the
accepted rows, and column identities must be distinct UUIDs. The fixture also
checks the logical type descriptor and that reads supply the same context to
BYOC as writes. Other logical descriptors and packed-value representations are
covered by the cell-type and native-row codec corpora respectively.

The implementation clears temporary plaintext buffers after encryption or
decoding, and transaction-owned initial epoch keys after submission failure,
commit or rollback. This reduces lifetime; it does not promise VM-wide erasure.
