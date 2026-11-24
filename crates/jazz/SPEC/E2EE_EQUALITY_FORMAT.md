# E2EE equality-token format

> **Status.** The normative reference implementation of this format is the
> pure-Rust crate `crates/jazz-e2ee`, with the independent generator
> `fixtures/e2ee-vectors.c`.

This companion specifies the built-in equality mechanism, the equality
context `C` and the value encoding `V`. Encrypted query integration is a
separate contract. The common envelope and context encodings are defined in
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
D = BLAKE2b-256(key=K, input=UTF8("jazz.e2ee.equality-key.v1") || 00 || len(H) || H || len(C) || C)
T = BLAKE2b-256(key=D, input=V)
stored token = H || T
```

The header binds the mechanism ID and version into the derived key. The common
layer supplies the context and the value; mechanisms do not choose the
identities or decide whether a candidate matches. Temporary derived-key
buffers are cleared after use.

## Context

`C = frame[ctx, T]` (`records::equality_context`): `ctx` is the canonical
context (`JE2C 01`) with application `Dₛ`, policy `jazz.e2ee.equality.v1`,
scope the space table's catalogue identity, identifier the space identifier,
table and column the column's catalogue identities, epoch the space epoch,
and empty row and recipient: a token does not bind its row, so equal values
of one column and epoch match across rows. `T` is the column's logical type
bytes, as for a cell record. The epoch must be a lowercase hyphenated UUID
of version 4 or 7 with the RFC variant (`[89ab]`), and the table and column
non-empty, as for cell contexts; anything else is `InvalidInput`.

## Value encoding

`V` (`records::equality_value`) is `00` for null, and otherwise `01`
followed by:

| Type                  | Bytes after `01`                                      |
| --------------------- | ----------------------------------------------------- |
| Boolean               | `00` or `01`                                          |
| 32- or 64-bit integer | eight bytes, big-endian two's complement              |
| Timestamp             | eight bytes, big-endian unsigned                      |
| Float (double)        | eight bytes, big-endian, of the canonical bit pattern |
| Text                  | the exact UTF-8 bytes                                 |
| Bytes                 | the bytes                                             |
| UUID                  | the 16 bytes                                          |
| Enum                  | the exact UTF-8 bytes of the value's variant name     |
| JSON (`json.v1`)      | the exact UTF-8 bytes of the stored JSON text         |
| Array (`array.v1`)    | the element count, then each element framed (below)   |

The canonical bit pattern of a float is a design choice of the token
encoding: every NaN is encoded as `7ff8000000000000` and `-0.0` as `0.0`, so
all NaNs share one token and `0.0` and `-0.0` share another. This is not
exactly Jazz's plaintext equality, which is not one rule either: stored
values compare by bits (so `0.0` and `-0.0` differ, and two NaNs with the
same bits are equal), while SQL comparisons use IEEE comparison (`0.0 =
-0.0`, and a NaN equals nothing), and the record reader refuses NaN values.
Since a token match only selects candidates, which the client decrypts and
compares with the query's own plaintext rule (see "Security and
compatibility"), the encoding errs towards matching: it never leaves out a
row that a plaintext rule would accept for `0.0`/`-0.0`, and NaN, which
plaintext rows cannot hold, needs no exact rule.
Text is compared by exact bytes: there is no Unicode, case or whitespace
normalisation, so `é` (`c3a9`) and `e` followed by U+0301 (`65cc81`) have
different tokens, as they are different Jazz text values. The column type is
bound by `T`, so values of different types are never compared. Fixtures
(asserted by the crate's `equality_values` test): `0.0` and `-0.0` both
encode as `010000000000000000`, every NaN as `017ff8000000000000`, `-2` as
`01fffffffffffffffe`, null as `00` and the empty text as `01`.
An enum value is encoded by its variant name, exactly as a text value (no
length framing, since `V` is the whole hash input, and no normalisation):
the variant `open` encodes as `016f70656e` and `done` as `01646f6e65`. The
column's type bytes `T`, which list the variants, keep an enum column's
tokens apart from a text column's.

A JSON value is hashed as the text Jazz stores, exactly as written: there is
no canonicalisation, trimming or key ordering, because Jazz's own equality on
a JSON column compares that text, so differently written JSON (`{"a":1}` and
`{"a": 1}`) are different values and have different tokens. `{"a":1}` encodes
as `017b2261223a317d`.

Clients write JSON text differently, so one value can have several stored
forms. The FFI (Go, Swift, Kotlin and the Rust client) re-serialises a JSON
value compactly with its object keys sorted, at every level, so
`{"b":1,"a":2}` is stored as `{"a":2,"b":1}`. TypeScript writes
`JSON.stringify` of the value, which keeps the keys in insertion order
(`{"b":1,"a":2}` stays so), and passes a string through as the text it is.
The same value written from Go and from TypeScript can therefore be stored
as different text, and then has different tokens: an equality search from one
misses rows the other wrote. It never returns a wrong row (a match is
decrypted and compared with Jazz's own rule), and plain Jazz equality on a
JSON column behaves the same way, since it compares the stored text. An app
that searches JSON from several languages should write it in one canonical
form (sorted keys, no extra whitespace) itself.

An array is encoded as `01`, its element count as four big-endian bytes, and
then, for each element in order, the length of the element's own encoding `V`
as four big-endian bytes followed by that `V` (a null element is the one byte
`00`, so its frame is `00000001 00`). The length frames keep `["a", "bc"]`
apart from `["ab", "c"]`; without them the concatenation would not be
injective. The elements are compared in order, as Jazz compares arrays. An
array's element type must itself have a value encoding, and nested arrays
apply the rule recursively. Fixtures (asserted by the crate's
`equality_values_of_json_and_arrays` test): `["a", "b"]` encodes as
`0100000002000000020161000000020162`, the empty array as `0100000000`,
`[null, "a"]` as `01000000020000000100000000020161`, and `[["a"], []]` as
`01000000020000000b0100000001000000020161000000050100000000`.

The JSON and array encodings are the version 1 value encodings of those
types, named `json.v1` and `array.v1`. They add types and change none of
the encodings above, so no existing token changes; the token mechanism
(`jazz.sodium.equality` 1) and the context policy (`jazz.e2ee.equality.v1`)
are as they were. Compatibility is defined here: older servers refuse deployments using the new column types, and changing either encoding requires a new versioned name because stored tokens cannot be recomputed.

## Column types

An equality index is declared per column, and its type decides the
encoding: boolean, integer (32-bit, values must fit), bigint, timestamp,
double, text, bytes, UUID, enum and JSON columns, and arrays of any of them
(`records::EqualityColumn::array_of`; arrays of arrays too), as in the table
above. Row and transaction-ID columns, and arrays of them, have no value
encoding and cannot be equality-indexed: a declaration naming one is refused
when it is declared, with a typed error that names the type
(`records::EqualityColumn::declare`, `UnsupportedEqualityColumn`: "Equality
indexes are not supported for row columns"), never at write time. Tokens are computed only through a declared
column (`records::equality_token_of`), which refuses a value of another
type with `InvalidInput`; there is no public entry point that hashes
caller-supplied bytes, so every token hashes the canonical encoding.

## Independent fixture

`fixtures/e2ee-vectors.c` constructs the envelope and derivation input
without any Jazz code and links against libsodium 1.0.22 as
`libsodium-sys-stable` =1.24.0 bundles it.
Assertions must remain enabled. It emits `equalityContext`, `equalityDerived`
and `equality`; the `jazz-e2ee` vector tests assert them.
Its public test inputs are a key containing bytes `00` through `1f`, the
existing context fixture, and UTF-8 `hello` as `V`.

This is an independent framing check, not an independent cryptographic
implementation or security audit. The generic fixture context tests the
adapter boundary; it is not a real application's table context.

## Security and compatibility

Tokens disclose equality and frequency within one column and epoch: the
server sees how many rows share each token, and which tokens queries ask
for. Applications must opt in before persisting searchable indexes. A token match identifies a candidate, not trusted plaintext: clients
must decrypt and compare candidates before applying logical limits or counts.
Jazz permissions remain authoritative for reads and writes.

Custom equality mechanisms (registered as in the crypto format's mechanism
agility rules) carry their own mechanism ID and version. They receive the
common layer's context and value bytes; they cannot change logical
comparison semantics. A provider failure is `CryptoFailure`, never a
non-matching token. Changes to the derivation, framing or value encoding
require an explicit compatibility decision before persisted indexes can use
them. There is no implicit reindexing or migration in this version.

A token is a one-way function of one value under one context and type. A
token of one value under a column's new context or type cannot be computed
from a token of the old one. So a column's tokens are written with its
values, and equality is declared when the table is created
([E2EE schema changes](E2EE_SCHEMA_CHANGES.md) §3.2).
