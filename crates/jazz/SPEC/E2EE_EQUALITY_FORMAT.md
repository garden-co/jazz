# E2EE equality-token format

This companion specifies the standalone built-in equality adapter. Table
contexts, logical-value encoding and encrypted query integration are separate
contracts and are not supplied by this layer. The common envelope and context encodings are defined in
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

Tokens disclose equality/frequency and query patterns, so applications must
opt in before persisting searchable indexes. A token match identifies a candidate, not trusted plaintext: clients
must decrypt and compare candidates before applying logical limits or counts.
Jazz permissions remain authoritative for reads and writes.

BYOC adapters retain their own explicit mechanism ID and version. They receive
the common layer's context and value bytes; they cannot change logical
comparison semantics. Changes to the derivation, framing or value encoding
require an explicit compatibility decision before persisted indexes can use
them. There is no implicit reindexing or migration in this version.
