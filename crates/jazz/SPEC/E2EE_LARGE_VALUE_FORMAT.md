# E2EE large-value stream format

> **Status.** The normative reference implementation of this format is the
> pure-Rust crate `crates/jazz-e2ee`, with the independent generator
> `fixtures/e2ee-vectors.c`. The earlier TypeScript implementation
> and the libsodium N-API primitives are historical and not normative.

This companion specifies the built-in large-value mechanism (the
`StreamCipher` role), not a new storage or transport protocol. Version 1 is the STREAM construction of
Hoang, Reyhanitabar, Rogaway and Vizár
([Online Authenticated-Encryption and its Nonce-Reuse Misuse-Resistance](https://eprint.iacr.org/2015/189.pdf))
over XChaCha20-Poly1305-IETF, in the big-endian 32-bit counter variant that
RustCrypto's `aead-stream` crate implements as `StreamBE32`. The `jazz-e2ee`
crate uses that crate's types; it contains no hand-written stream
construction. It replaces the earlier libsodium secretstream mechanism
`jazz.sodium.stream`, which was never released and is not read. A custom
stream mechanism is registered like any other (crypto format, mechanism
agility).

## Encoding, version 1

The mechanism is `jazz.stream.xchacha20poly1305`, version 1. Let `H` be its
[common envelope header](E2EE_CRYPTO_FORMAT.md#common-envelope-version-1), with
no payload, and `C` the caller-supplied authenticated context bytes.
All lengths below are unsigned four-byte big-endian integers.

Define `A = length(H) || H || length(C) || C`. Derive the 32-byte stream key
`K` with keyed BLAKE2b, using the supplied 32-byte epoch key as key and `A` as
input. Every record also uses `A` as associated data. The mechanism
ID/version therefore separates this derivation from other purposes. The common
E2EE layer, not the cipher adapter, resolves the context's identities.

Each value has a fresh random 19-byte nonce prefix `P`. Record `i` (counting
from 0) is XChaCha20-Poly1305-IETF under `K` with the 24-byte nonce
`P || u32be(i) || f`, where the flag byte `f` is 1 for the last record and 0
otherwise. Because the key is fixed per epoch and context, uniqueness of
nonces rests on `P`: 152 random bits per value.

The encoded stream is:

1. `H`, followed by `P`.
2. Zero or more data records: ciphertext length, then ciphertext and its
   16-byte tag, with flag 0.
3. Exactly one last record: length 16, then the tag of an empty plaintext,
   with flag 1.

Data records contain 1–65,536 plaintext bytes, so their lengths are
17–65,552; a length of 16 therefore identifies the last record before it is
decrypted. Writers coalesce input into 65,536-byte records, with a shorter
last data record when needed. Readers accept any non-empty data record within
this bound. Empty values still contain the envelope, the nonce prefix and the
last record. The index in the nonce makes dropped, reordered, duplicated or
spliced records fail authentication; the last flag makes a truncated stream
fail, since a data record never authenticates as the last.

Transport boundaries have no cryptographic meaning. A length, header or
ciphertext record may span any number of upstream chunks. Readers reject
unknown envelope versions or mechanisms, lengths other than 16 or
17–65,552, incomplete headers/records, authentication failures, a stream
without its last record and any bytes after the last record.

## Fixture

`fixtures/e2ee-vectors.c` builds this stream directly with libsodium's
XChaCha20-Poly1305 and explicitly constructed nonces, independently of the
`aead-stream` implementation. With the generator's key (bytes `00`–`1f`),
context fixture and nonce prefix `00`–`12`, it encrypts `hello` as record 0,
`hello` as record 1 and the empty last record 2. Its key derivation output
(`streamDerived`) is
`fb300f65cfa0e0dbc3a0c675375df7d2bcc2fbfa79dd09796509df7b6d3c8130` and the
stream is
`4a453245011d6a617a7a2e73747265616d2e786368616368613230706f6c793133303500000001000102030405060708090a0b0c0d0e0f10111200000015f379dad1393c199f0448eb4c646e414886a6bfdae500000015cc8de564b4a5ae9a96cebee6da64c20875e9ac682b00000010361c27bcd9b94847dd16094baaa17860`.
The `jazz-e2ee` writer, given the same inputs and the single input `hello`,
produces
`4a453245011d6a617a7a2e73747265616d2e786368616368613230706f6c793133303500000001000102030405060708090a0b0c0d0e0f10111200000015f379dad1393c199f0448eb4c646e414886a6bfdae500000010a3a6e6facd9827eb4d173bb309517cb0`.
Both are asserted by the crate's vector tests, which also rebuild the stream
from libsodium on random inputs.

## Stream record, version 1

A stored encrypted large value is a routing record whose payload runs to the
end of the stored value. The routing record is a common envelope with
mechanism `jazz.e2ee.stream-record`, version 1, whose payload is:

| Field         | Encoding                                                                         |
| ------------- | -------------------------------------------------------------------------------- |
| Epoch         | Exactly 36 ASCII bytes: the space epoch's lowercase UUID                         |
| Cipher output | Every remaining byte of the stored value: the stream cipher's output, unmodified |

The stream mechanism's header `H` is carried once: the cipher output is the
[encoding above](#encoding-version-1), which begins with `H`. Readers strip
the outer header and the 36 epoch bytes, check that the remainder is an
envelope of the installed stream mechanism (another mechanism reports
`unsupported-format`), and hand the whole remainder, `H` included, to the
cipher, which checks `H` again. This mirrors the
[cell record](E2EE_CRYPTO_FORMAT.md#cell-record-version-1), whose payload is
the epoch followed by the cell cipher's own envelope. The payload is at least
47 bytes.

The cipher's context `C` is
`frame[frame[ctx, T], H]`: `ctx` is the canonical context of a
[cell record](E2EE_CRYPTO_FORMAT.md#cell-record-version-1) with policy
`jazz.e2ee.stream-record.v1` instead, `T` the same logical type bytes, and
`frame` u32be-length-prefixed concatenation. The distinct policy separates a
stream from a cell of the same coordinates. Only non-indexed `bytes` columns
may use stream records; any other column reports `unsupported-format`.

A reader first tries the cell record; on `unsupported-format` it tries the
stream record. Wrong mechanism, a payload under 47 bytes, or a non-UUID epoch
fail as for cells.

Framing example, epoch `00000000-0000-4000-8000-000000000000`: the record
begins
`4a45324501176a617a7a2e653265652e73747265616d2d7265636f72640000000130303030303030302d303030302d343030302d383030302d303030303030303030303030`
and continues directly with the stream bytes, which start with
`4a453245011d6a617a7a2e73747265616d2e786368616368613230706f6c793133303500000001`
(`H`). This
example is pinned by this document and asserted by the `jazz-e2ee` vector
tests.

## Consumption and lifetime

Each plaintext record is returned only after it authenticates. A caller may
consume this prefix, but must not report whole-value success until the
empty last record has authenticated and the input has ended. A missing or
corrupted last record fails even after plaintext has been delivered. Wrong
keys or contexts fail authentication (`InvalidCiphertext`); a provider
failure in an update or finish step is `CryptoFailure` and propagates
unchanged through the record helpers.

Because every record authenticates on its own (its index and last flag are
in the nonce), a reader could in principle decrypt a range by seeking to a
record boundary; the first release does not offer range reads on encrypted
columns (core hooks note, H5), but the format does not prevent them.

Working memory is bounded by record buffers, context and the current input
chunk; no value-sized plaintext buffer is required. Dropping or cancelling a
sealer or opener clears its derived key and owned state. Each replacement
starts a fresh stream with a new nonce prefix and does not read, diff or
merge the previous plaintext.

## Qualification and integration boundary (historical)

The earlier TypeScript implementation's suites covered
interoperability, multiple records, empty values, backpressure,
cancellation, truncation, tampering, wrong keys or contexts and trailing
bytes. The Rust implementation must carry the equivalent cases in
`jazz-e2ee`'s tests and conformance kit.

The [fixture](#fixture) is generated directly with libsodium, without the
`aead-stream` implementation or Jazz's framing helpers. This is independent
format evidence, not an independent cryptographic implementation or security
audit.

Generic large-value staging, atomic owner-row publication, locator delivery
and storage remain governed by [chapter 19](19_large_values.md); the
encrypted streaming integration is specified in the core hooks note (H5).

### Extraction qualification boundary (historical)

The regression suites and independent byte corpus above are carried from the
repaired source snapshot. No build, formatter, linter or test was run during
this extraction. The integrating checkout must build its own native/WASM
artifacts and run the Node, Chromium and package qualification commands; no
artifact or qualification receipt from another checkout is inherited.

This is the standalone cipher contract, not full landing or security approval.
Generic large-value publication/transport integration remains separate; no
claim of end-to-end encrypted file upload readiness is made here.
Release-level qualification and the remaining generic large-value integration
boundary are tracked in [#3125](https://github.com/garden-co/jazz2/issues/3125).
