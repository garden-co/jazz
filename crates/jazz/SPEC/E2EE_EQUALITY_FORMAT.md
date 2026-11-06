# E2EE equality-token format

This companion to [chapter 20](20_e2ee.md) specifies the built-in equality
adapter. The common envelope and context encodings are defined in
[E2EE cryptographic formats](E2EE_CRYPTO_FORMAT.md).

## Built-in token mechanism, version 1

The mechanism ID is `jazz.sodium.equality`, version `1`. Its common `JE2E`
envelope contains exactly 32 payload bytes. Tokens are deterministic: there
is no nonce. The mechanism uses libsodium's keyed BLAKE2b with 32-byte outputs.

Let `K` be the 32-byte space epoch secret, `H` the common envelope header
with this mechanism and no payload, `C` the operation context bytes, and `V`
the encoded logical value. `len(X)` is the byte length of `X`, encoded as an
unsigned four-byte big-endian integer. There is no padding or field count.

```text
D = BLAKE2b-256(key=K, input=len(H) || H || len(C) || C)
T = BLAKE2b-256(key=D, input=V)
stored token = H || T
```

The header binds the adapter ID and version into the derived key. The common
layer supplies a separate equality purpose and the applicable identities;
adapters do not choose those identities or decide whether a candidate matches.
Temporary derived-key buffers are cleared after use. This reduces their
lifetime but does not guarantee erasure in JavaScript runtimes.

## Operation context and value boundary

The table integration length-frames two fields: a `JE2C` version-one context,
then the encrypted column's encoded logical type. The context uses policy
`jazz.e2ee.equality.v1`, the application identity, the space's scope and
identifier, stable table and column identities, and the space epoch ID.
Row and recipient fields are empty: equal values in different rows of the
same indexed column and epoch must produce the same token.

The value encoding must agree with Jazz equality. Numeric `-0` and `0` use
the same index encoding; cell encryption preserves the original value.
Normalisation applies recursively to array elements and enum payload fields.
It does not sort arrays, reorder JSON properties or normalise Unicode.

## Logical value bytes

`V` is the native packed-record encoding of one logical column, after the
normalisation above. Use the logical descriptor, before replacing encrypted
columns with `BYTEA`. The single-column record has no row ID, field count,
type tag or offset table. Its type is bound separately in `C`.

All lengths and offsets in this section are unsigned 32-bit little-endian
integers. They differ from the big-endian context framing described above.

| Logical type           | Encoded value                                                                |
| ---------------------- | ---------------------------------------------------------------------------- |
| Boolean                | One byte: `00` or `01`.                                                      |
| Integer                | Signed 32-bit, little-endian two's complement.                               |
| Bigint                 | Signed 64-bit, little-endian two's complement.                               |
| Timestamp              | Eight-byte little-endian integer representation of Unix milliseconds.        |
| Finite double          | IEEE 754 binary64, little-endian; both signed zeros encode as positive zero. |
| UUID                   | Sixteen UUID bytes; hex spelling and case are not retained.                  |
| Text, JSON, plain enum | `02` followed by UTF-8 bytes. JSON uses the ordinary stored JSON text.       |
| Bytes                  | `02` followed by the bytes.                                                  |

A nullable present value has a leading `01` before its ordinary encoding.
A nullable absent value is all zero bytes: one plus the underlying width for
fixed-width types, or a single `00` for variable-width types. Null and empty
text therefore have different encodings.
A sparse column adds an outer presence marker: present values, including an
explicit null, have a leading `01`; missing values use the outer nullable
encoding. This records the byte layout, not qualification of sparse queries.

Arrays preserve element order. Fixed-width elements are concatenated without
a count. Variable-width arrays encode the element count, then the end offsets
of all but the final element, then each encoded element. Offsets are measured
from the beginning of the array encoding. An empty fixed-width array has zero
bytes; an empty variable-width array is `00000000`.

Payload enums encode the UTF-8 case-name length, the case name, and the payload
record. The payload record starts with `00` (no row ID), then its byte length,
then its packed fields. Fixed-width fields come first in schema order,
followed by end offsets for all but the final variable-width field, followed
by the variable-width fields in schema order. These offsets are relative to
the start of the packed fields, excluding the no-ID marker and byte length.

Query JSON is an intermediate representation, not `V`. Decimal bigint strings
are restored using the column type, including inside arrays and enum payloads.
Timestamp strings use the same parser as ordinary Jazz queries before value
encoding. Decryption encodes authenticated indexed values into independently
owned comparison bytes before exposing public results. Verification reuses
those private bytes. Re-encoding the public JSON
object is not equivalent: parsing has already discarded the stored text's
whitespace. Public byte arrays cannot mutate the private comparison bytes.
The private bytes follow the row's lifetime and are also retained
when subscription reducers copy a decoded row; they are not public row fields.
Their reclamation depends on garbage collection, not explicit zeroisation.

`packages/jazz-tools/src/e2ee/equality-values.test.ts` pins 25 hand-authored
byte literals for scalar boundaries, signed zeros, UTF-8, nullable values,
arrays and payload enums. The same tests run in Node and Chromium. They are
not generated snapshots or an exhaustive proof of every logical value.
Public Node integration tests additionally verify that explicit null and
omitted optional strings match null equality, while empty text remains
distinct. JSON queries preserve ordinary text equality, including property
order and whitespace, through reads, transactions and subscription updates.
Non-finite numbers in encrypted predicates fail explicitly before query JSON
can convert them to null. Sparse markers describe an internal storage carrier,
not a separate public schema/query feature. Chromium exercises full and projected
equality results and their subscription updates; the broader logical-value
matrix is Node-hosted. Byte fixtures alone do not establish query behaviour.

## Independent fixture

`fixtures/e2ee-vectors.c` constructs the envelope and derivation input without
Jazz TypeScript helpers and links against the pinned libsodium 1.0.22 archive.
Assertions must remain enabled. It emits `equalityContext`, `equalityDerived`
and `equality`; the stored literals are in the TypeScript crypto fixture.
Its public test inputs are a key containing bytes `00` through `1f`, the
existing context fixture, and UTF-8 `hello` as `V`.

Browser and native adapter tests compare the token against the same literal.
This is an independent framing check, not an independent cryptographic
implementation or security audit. The generic fixture context tests the
adapter boundary; it is not a real application's table context.

## Security and compatibility

Tokens disclose equality/frequency and query patterns, so indexes remain
opt-in. A token match identifies a candidate, not trusted plaintext: clients
must decrypt and compare candidates before applying logical limits or counts.
Jazz permissions remain authoritative for reads and writes.

BYOC adapters retain their own explicit mechanism ID and version. They receive
the common layer's context and value bytes; they cannot change logical
comparison semantics. Changes to the derivation, framing or value encoding
require an explicit compatibility decision before persisted indexes can use
them. There is no implicit reindexing or migration in this version.

Release qualification and independent security review remain tracked in
[#3125](https://github.com/garden-co/jazz/issues/3125); these format fixtures do not establish either.
