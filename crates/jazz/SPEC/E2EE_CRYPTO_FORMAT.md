# E2EE cryptographic formats

This companion specifies the standalone context/envelope codecs and browser/native
cryptographic adapters. Byte-level fixtures pin the encoding independently of
the implementation's serializer. Table writes and lifecycle records are not
part of this layer. The [stream format](E2EE_LARGE_VALUE_FORMAT.md) specifies
the bounded large-value cipher. Test descriptions below identify carried
coverage, not a validation receipt for this extraction.

## Common envelope, version 1

All multi-byte integers are unsigned, big-endian. There is no padding.

| Field               | Encoding                                                 |
| ------------------- | -------------------------------------------------------- |
| Magic               | Four bytes: `4a 45 32 45` (`JE2E`)                       |
| Envelope format     | One byte: `01`                                           |
| Mechanism ID length | One byte, 1–64                                           |
| Mechanism ID        | Exactly that many ASCII bytes, characters `[a-z0-9.-]`   |
| Mechanism version   | Four bytes, non-zero                                     |
| Mechanism payload   | Remaining bytes; interpretation belongs to the mechanism |

The reader requires the installed adapter's exact ID and version. Unknown
formats, mismatched mechanisms, invalid IDs and truncated headers fail closed.
This header is routing metadata, not an authentication claim. Mechanism
implementations must authenticate their ID/version with the operation context.
Payload validation and authentication happen before plaintext is returned.

Example fixture: mechanism `test`, version 1, payload `aa`:
`4a45324501047465737400000001aa`.

## Canonical context version 1

Prefix: `4a 45 32 43 01` (`JE2C`, version 1). Then nine fields in this exact
order: application, policy, scope, identifier, table, row, column, epoch, recipient.
Each field is a four-byte unsigned big-endian byte length followed by its UTF-8
bytes. Application, policy, scope, identifier and epoch must be non-empty.
Unused table/row/column/recipient fields have zero length; omitted and empty
optional fields mean the same thing. Unknown fields are rejected. Each field
is limited to 65,535 UTF-8 bytes. Unpaired UTF-16 surrogates are rejected rather
than replaced; no Unicode, case or identifier normalisation takes place.

The common E2EE layer supplies all IDs applicable to the operation; cell contexts
include table, row and column. Key contexts identify their owner scope and epoch
and relevant recipient. The codec pins representation, not identity resolution
or authorisation. Adapters receive these bytes and cannot reinterpret routing.

## Built-in mechanisms

### Cell mechanism version 1

The mechanism ID is `jazz.sodium.cell`, version 1. Its payload is a fresh random
24-byte nonce followed by libsodium's combined XChaCha20-Poly1305-IETF ciphertext
and 16-byte authentication tag. Empty plaintext is valid. Fewer than 40 payload
bytes is malformed. Replacements generate fresh nonces.

Let `H` be the common envelope header with this mechanism and no payload, and
`C` be the caller's canonical operation context. Associated data is `H || C`.
The 32-byte cell key is keyed BLAKE2b with a 32-byte input epoch secret as key,
32-byte output, and input `UTF8("jazz.e2ee.cell-key.v1") || 00 || H || C`.
This binds the mechanism and purpose as well as the supplied context. The common
layer will supply the full application/policy/space/row/table/column/epoch context;
the adapter does not resolve those identities. Derived key buffers are cleared
after use; this is best-effort lifetime reduction, not guaranteed VM erasure.

## Built-in key envelope v1

Mechanism `jazz.sodium.key`, version 1, carries exactly one 32-byte key. The
first payload byte identifies the operation: 1 for symmetric wrapping, 2 for a
device sealed box. Readers reject other operations and incompatible lengths.
The interface is independent of CellCipher; custom key envelopes do not change
cell encryption or common context semantics.

For symmetric wrapping, associated data is the common envelope header, byte 1,
then canonical context. Derive a 32-byte key with keyed BLAKE2b using the wrapping
key and input `UTF8("jazz.e2ee.wrap-key.v1") || 0x00 || associatedData`.
The separator is one zero byte. The payload is byte 1, a fresh 24-byte
nonce, and XChaCha20-Poly1305-IETF ciphertext with its appended 16-byte tag.
Its length is exactly 73 bytes. This purpose label separates wrapping from cells.

For device envelopes, generate a libsodium crypto_box keypair. The payload is
byte 2 followed by a libsodium sealed box. Its authenticated plaintext is the
common envelope header, byte 2, uint32 big-endian context byte length, context,
and the 32-byte key. On opening, verify the entire prefix and exact length before
returning any key bytes. This binds context despite sealed boxes lacking a
separate associated-data input. A sealed box authenticates its contents to the
recipient, not a sender identity; ordinary Jazz authorisation remains required.

Browser and native adapters implement both operations; both-direction exchanges
and direct Rust input checks are tested. `fixtures/e2ee-vectors.c` independently
constructs the framing and derivation inputs against libsodium 1.0.22. Its stored
vectors cover context, cell encryption, symmetric wrapping and device envelopes in
Node and Chromium. Browser tests additionally check writer output against the
independent derivation inputs and reject a flipped bit at every envelope position.
This is independent protocol framing, not an independent crypto implementation or
security audit. Sealed-box generation is random; its stored fixture is fixed.
The generator must be compiled with assertions enabled; `NDEBUG` is rejected.

## Built-in device signatures, version 1

Mechanism `jazz.sodium.sign`, version 1, uses libsodium's single-part detached
Ed25519 signatures, not Ed25519ph. Let `H` be its common envelope header with
no payload and `R` the common layer's canonical lifecycle-record bytes. Sign
exactly `H || R`; store the 64-byte detached signature as the envelope payload.
Verification checks the exact mechanism/version, payload length and signature.
The fixed header domain-separates signatures from other protocol messages.

Public keys contain 32 bytes. Private keys use libsodium's 64-byte seed/public-key
representation. Signing reconstructs the keypair from its seed and rejects an
inconsistent public-key half; it clears that temporary private-key buffer without
modifying the caller's key. New signing keys use independent secure randomness.

`DeviceSigner` exposes `createKeyPair()`, `sign(privateKey, record)` and
`verify(publicKey, record, signature)` as asynchronous methods. Verification
returns false for invalid inputs/signatures; signing rejects malformed keys.
This adapter authenticates bytes, not signer eligibility or accepted membership.
The lifecycle's canonical signed-record layout and durable enrolment format
remain to be implemented and pinned before persistence.

The browser and native factories are implemented. A Node-hosted test uses the RFC 8032 test-one
key with Node/OpenSSL to independently verify the framing and signature, and
checks altered envelopes, records and keys. Native/browser exchanges verify both
directions with keys generated by each implementation. Direct NAPI checks cover
malformed seeds, secret keys, public keys and signatures, plus empty messages.
Independent default/override selection is tested on both platforms; Chromium
also verifies a fixed OpenSSL signature and rejects a changed signature.
Device-lifecycle integration remains unqualified. Native coverage is on macOS
arm64, not every native platform.
This receipt is not a protocol audit or a completed revocation implementation.

## Adapter selection

`JazzCrypto` independently overrides `cellCipher`, `keyEnvelope`, `deviceSigner`,
`equalityIndex` and `largeValueCipher`. Browser and native factories initialise
only omitted platform defaults. The browser dependency is loaded lazily. Selection validates
mechanism IDs and versions; this does not validate an adapter's cryptographic
strength. Import interfaces and context/envelope codecs from `jazz-tools/e2ee`.
Platform factories are explicit: `jazz-tools/e2ee/browser` and
`jazz-tools/e2ee/native`. Packed public imports, declaration resolution and
browser/native exchange have executable coverage; Chromium also reads fixtures
through the built browser bundle. After building correctness artifacts, run
`pnpm --filter jazz-tools build:runtime` then
`pnpm --filter jazz-tools test:crypto-package`.

`LargeValueCipher.encrypt/decrypt` transform `AsyncIterable<Uint8Array>` with key
bytes, canonical context and an optional abort signal. Pulling respects
backpressure; implementations bound memory, return owned output chunks and
propagate cancellation and errors. Successful iteration to the end requires
final authentication; consumed prefixes alone do not establish whole-stream
success. Both platform factories supply the built-in
[version-one stream mechanism](E2EE_LARGE_VALUE_FORMAT.md) by default.
An explicitly supplied adapter bypasses that platform default; there is no
plaintext or whole-buffer fallback.

## Dependency qualification and limits

The published [Cryptography Engineering assessment](https://www.privateinternetaccess.com/blog/wp-content/uploads/2017/08/libsodium.pdf)
reviews libsodium 1.0.12 and 1.0.13, including XChaCha20-Poly1305 and sealed
boxes. It is historical implementation audit evidence, not a review of the
current release, JavaScript/Rust bindings or Jazz's protocol.

Pinned matching pair: `libsodium-sys-stable` 1.24.0 and the official
`libsodium` JavaScript package 0.8.3. Its upstream libsodium submodule is
`77e1ce5d6dee871c49ef211222ba18ef0c486bda`. Inspection found all 316 C/header/
assembly source files under `src/libsodium` identical to the Rust crate's bundled
`LATEST.tar.gz` (SHA-256
`b20a92e7ec25b285eafa349d721a5bb27e3a8ba94c0816630a127883f1d1b3ab`).
The browser packages `libsodium` and `libsodium-wrappers` 0.8.3 are now exact
development pins; the native dependency is pinned to exactly 1.24.0. Native cell
interoperability with Node-hosted libsodium.js is tested in both directions.
The installed Rust crate's bundled archive matches the SHA-256 above. The wrapper's
upstream dependency range is not an exact pin, so the published browser implementation
bundles the locked dependencies rather than letting consumers resolve that range.
`bundle-e2ee-crypto.mjs` checks both versions and qualified JavaScript source hashes,
then verifies that esbuild included those files. The distribution includes
`SODIUM-SOURCES.json` and full licence notices. No runtime sodium npm dependency
is left for consumers to resolve.

The original source receipt covers Chromium and native Node on macOS arm64;
this extraction has not rerun that qualification. Other native
platform builds and alternative system-library/build overrides are not covered
by this receipt and must qualify their actual libsodium source and configuration.
This is interoperability and input-rejection evidence, not an independent binding
or protocol security audit.

JavaScript 0.8.4 instead points at
`33cc75ab1565d9dcbe808354191bd572ad6b64d0`; comparison found a difference in
`sodium/core.c`. The change fixes misuse-handler callback re-entry while a global
lock is held. The pinned pair predates that fix; no Jazz custom misuse handler is
installed. Updating either side requires requalifying the matching pair and vectors;
matching version labels alone are insufficient.

This layer pins the context, derivation and envelope formats with independent
fixtures and rejection tests. Persistent application writes require separate
key lifecycle and table integration; neither is supplied by these adapters.
