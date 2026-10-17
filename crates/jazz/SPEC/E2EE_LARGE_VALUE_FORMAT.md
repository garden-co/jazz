# E2EE large-value stream format

This companion specifies the built-in `LargeValueCipher` and its common
encrypted BYTEA cell record. It adds no storage or transport protocol.
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
Cancellation also performs this cleanup while the consumer is paused at a
yielded header or data record; another pull or explicit `return()` is not
required. A subsequent pull rejects with the original abort reason, including
when encryption was paused at its final record or decryption was accepting EOF.
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

## Encrypted BYTEA stream record, version 1

The common layer stores the `jazz.e2ee.stream-record`, version 1 envelope
header, followed by the canonical epoch UUID as 36 lowercase ASCII bytes,
then the selected `LargeValueCipher` mechanism's common envelope header
(without payload), followed by the adapter's complete output. The mechanism
header is mandatory even for custom adapters whose output has no envelope.
The built-in adapter therefore retains its own inner header as well.

The outer header bytes are
`4a45324501176a617a7a2e653265652e73747265616d2d7265636f726400000001`.
The UUID encoding is identical to [cell records](E2EE_CELL_FORMAT.md).
`stream-record.test.ts` pins this header, UUID and an independent BYOC
mechanism/version header against literal bytes.

Authenticated context is a u32be-length frame of two fields:
the complete two-field cell context with policy replaced by
`jazz.e2ee.stream-record.v1`, and the exact adapter mechanism header.
Thus application, scope, space identifier, stable table/column identity,
row, epoch, logical type and adapter mechanism are authenticated. The
plaintext is raw logical BYTEA bytes, not a packed native row. Empty files
are not null. Legacy cell records remain readable; ordinary and streamed
whole-value replacements may use either record format.

Ordinary row reads dispatch centrally by the outer record mechanism.
They parse headers with views rather than copying the complete ciphertext,
consume decryption through final authentication and EOF, and only then
return the assembled BYTEA value. Failed or truncated reads clear owned
plaintext chunks and expose no partial row. This complete-value read may
allocate the plaintext value; upload encryption remains bounded streaming.

Uploads support root-view logical BYTEA only. Explicit branches, implicit
branch coordinates, encrypted indexed streamed columns, Text/JSON streams,
streamed scope-row values and incremental editing are rejected. Plaintext
streaming remains unchanged.

## Publication and key lifetime

Source consumption happens before the final exclusive transaction. A
private plan borrows a ready existing epoch key, or owns an unpublished
initial-space secret and signed root/grants. New scopes and legacy first
files publish the exact planned seed with their owner rows atomically.
Initial explicit recipient sets do not implicitly include the creator.
The accepted initial write then awaits the narrow original-author recipient
handoff described in [space lifecycle formats](E2EE_SPACE_FORMAT.md).
Delivery uses a later authority transaction; failed delivery preserves the
accepted owner receipt with a bounded maintenance warning. Explicit
`db.e2ee.explain()` can retry; acceptance alone is not delivery readiness.

Final preparation replays space, account/device and effective group
membership through the transaction-backed history reader. Epoch or
membership changes reject publication, including when ordinary writes use
`staleWrites: "warn"`. Initial plans revalidate author and recipient epochs
and exact root absence. Seeds are Db/scope-bound and single-use, and their
secret is cleared after handoff or disposal. No source or encryption replay
is performed. Authority rejection retains ordinary rejected-version bytes
for retry/discard; retry must retain these strict read preconditions.

Caller source/staging failures retain their original identity. Key-adapter
failures during preparation or final plan validation are bounded to
`key-unavailable`; stream-cipher failures (including synchronous construction
and later iteration) are bounded to `encryption-failed`. Raw adapter
diagnostics may contain secrets and are not attached as error causes.

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

Generic large-value staging, atomic owner-row publication, wait completion,
locator delivery and storage remain governed by [chapter 19](19_large_values.md).
Cipher tests alone do not qualify publication. The encrypted upload and
streaming-space integration tests exercise the shared lifecycle and public
write path. No new storage backend, range-read or incremental-edit interface
is introduced. Release-level qualification and independent security review
remain tracked in [#3125](https://github.com/garden-co/jazz2/issues/3125).
