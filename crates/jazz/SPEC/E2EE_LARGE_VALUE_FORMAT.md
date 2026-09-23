# E2EE large-value stream format

This companion specifies the standalone built-in `LargeValueCipher`, not a
new storage or transport protocol.
Browser and native Node use pinned libsodium 1.0.22 secretstream
XChaCha20-Poly1305. A custom `largeValueCipher` replaces this adapter independently
of the other BYOC adapters.

## Encoding, version 1

The mechanism is `jazz.sodium.stream`, version 1. Let `H` be its
[common envelope header](E2EE_CRYPTO_FORMAT.md#common-envelope-version-1), with
no payload, and `C` the caller-supplied authenticated context bytes.
All lengths below are unsigned four-byte big-endian integers.

Define `A = length(H) || H || length(C) || C`. Derive the 32-byte stream key
with keyed BLAKE2b, using the supplied 32-byte epoch key as key and `A` as
input. Every secretstream record also uses `A` as associated data. The
mechanism ID/version therefore separates this derivation from other purposes.
The common E2EE layer, not the cipher adapter, resolves the context's identities.

The encoded stream is:

1. `H`, followed by the fresh 24-byte secretstream header.
2. Zero or more data records: ciphertext length, then secretstream ciphertext.
3. Exactly one final record: length 17, then the authenticated empty final record.

Data records use libsodium `TAG_MESSAGE` (0), contain 1–65,536 plaintext
bytes and add 17 authentication bytes. Writers coalesce input into 65,536-byte
records, with a shorter last data record when needed. Readers accept any
non-empty data record within this bound. The final record uses `TAG_FINAL`
(3) and has no plaintext. Empty files still contain the envelope, fresh
secretstream header and final record. `TAG_PUSH` and `TAG_REKEY` records
are not accepted by this format; libsodium's automatic internal rekeying
remains part of secretstream.

Transport boundaries have no cryptographic meaning. A length, header or
ciphertext record may span any number of upstream chunks. Readers reject
unknown envelope versions or mechanisms, lengths outside 17–65,553,
incomplete headers/records, authentication failures, empty data records,
non-empty final records and any bytes after the final record.

## Consumption and lifetime

Each plaintext record is returned only after authentication. A caller may
consume this prefix, but must not report whole-file success until iteration
exhausts naturally after authenticating the empty final record and reaching
upstream EOF. Missing or corrupted final authentication fails even after
plaintext has been delivered. Wrong keys or contexts fail authentication.
A successful early `return()` or `break` does not establish whole-file success.

The adapters pull input only as needed for the current record. Working memory
is bounded by record buffers, context and the current upstream chunk; an
upstream producer can itself supply an arbitrarily large chunk. No file-sized
plaintext collection is required. Aborting the supplied `AbortSignal` interrupts
a pending read. Consumer `return()` does not interrupt an already pending
`next()`; use the signal for that case.

On early consumer return (including `break`), cancellation or failure, the
adapters clear owned crypto state and request upstream cleanup at most once.
They do not await the upstream `return()` promise, even without an
`AbortSignal`. Cleanup rejections and synchronous cleanup errors are ignored;
they must not replace successful early termination or the original failure.
The source remains responsible for stopping its own I/O. Ordinary source
read errors propagate. Natural exhaustion requires no additional cleanup call.

Derived key buffers and owned crypto state are cleared on exit. Rust owns its
state until explicit disposal or drop; the browser adapter clears and frees
the WASM state allocation. JavaScript buffer clearing reduces lifetime but
does not guarantee erasure of VM copies. Each replacement starts a fresh
secretstream and does not read, diff or merge the previous plaintext.

## Qualification and integration boundary

`packages/jazz-tools/src/e2ee/large-value-cipher.test.ts` covers native/browser
interoperability, independent fixture decryption, multiple records, empty
values, whole-value replacement, backpressure, source errors, cancellation,
truncation, tampering, wrong keys/context and trailing bytes.
`packages/jazz-tools/tests/browser/e2ee-large-value.test.ts` checks the built
package in Chromium, including a 1 MiB stream and stalled source cleanup.

`fixtures/e2ee-vectors.c` generates the fixed stream fixture directly with
libsodium, without Jazz's TypeScript framing or derivation helpers. The
stored fixture contains two `hello` records and an empty final record. Its
random stream header is pinned in `src/e2ee/fixtures/vectors.ts`; regenerating
it produces another valid ciphertext. This is independent format evidence,
not an independent cryptographic implementation or security audit.

This stage implements the cipher contract only. Generic large-value staging,
atomic owner-row publication, wait completion, locator delivery and storage
remain governed by [chapter 19](19_large_values.md). Their streaming/replacement
integration is not established by these cipher tests. No whole-value buffering
fallback, storage backend, range-read or incremental-edit API is introduced.

### Extraction qualification boundary

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
