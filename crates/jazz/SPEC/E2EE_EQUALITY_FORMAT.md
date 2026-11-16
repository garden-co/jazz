# E2EE equality-token format

> **Status.** The normative reference implementation of this format is the
> pure-Rust crate `crates/jazz-e2ee`, with the independent generator
> `fixtures/e2ee-vectors.c`. The earlier TypeScript implementation
> and the libsodium N-API primitives are historical and not normative.

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
bytes, as for a cell record. The epoch must be a lowercase UUID and the
table and column non-empty, as for cell contexts; anything else is
`InvalidInput`.

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

The canonical bit pattern of a float makes tokens match Jazz's plaintext
equality: every NaN is `7ff8000000000000` and `-0.0` is encoded as `0.0`.
Text is compared by exact bytes: there is no Unicode, case or whitespace
normalisation, so `é` (`c3a9`) and `e` followed by U+0301 (`65cc81`) have
different tokens, as they are different Jazz text values. The column type is
bound by `T`, so values of different types are never compared. Fixtures
(asserted by the crate's `equality_values` test): `0.0` and `-0.0` both
encode as `010000000000000000`, every NaN as `017ff8000000000000`, `-2` as
`01fffffffffffffffe`, null as `00` and the empty text as `01`.

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
