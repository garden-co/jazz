# E2EE cryptographic formats

> **Status.** The normative reference implementation of these formats is the
> pure-Rust crate `crates/jazz-e2ee`, with the independent generator
> `fixtures/e2ee-vectors.c`; the protocol on top is
> [E2EE protocol](E2EE_PROTOCOL.md).

This companion specifies the context and envelope codecs and the built-in
mechanisms. Byte-level fixtures pin the encoding independently of the
implementation's serializer. Table writes and lifecycle records are specified
by the protocol. The [stream format](E2EE_LARGE_VALUE_FORMAT.md) specifies the
bounded large-value cipher.

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

The reader requires the installed mechanism's exact ID and version. A header
that is malformed (truncated, another magic or format byte, an ID length
outside 1–64, an ID byte outside `[a-z0-9.-]`, or version 0) is
`invalid-ciphertext`; a well-formed header of a mechanism that is not
registered or not allowed is `unsupported-format`. Both fail closed. This
header is routing metadata, not an authentication claim. Mechanism
implementations must authenticate their ID/version with the operation
context; the conformance kit checks it (below).
Payload validation and authentication happen before plaintext is returned.

Example fixture: mechanism `test`, version 1, payload `aa`:
`4a45324501047465737400000001aa`.

## Canonical context version 1

Prefix: `4a 45 32 43 01` (`JE2C`, version 1). Then nine fields in this exact
order: application, policy, scope, identifier, table, row, column, epoch, recipient.
Each field is a four-byte unsigned big-endian byte length followed by its UTF-8
bytes. Application, policy, scope, identifier and epoch must be non-empty.
Unused table/row/column/recipient fields have zero length; omitted and empty
optional fields mean the same thing. Each field is a Rust `&str`, so it is
valid UTF-8 by construction, and is limited to 65,535 bytes; no Unicode, case
or identifier normalisation takes place.

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

### Cell record, version 1

A stored encrypted cell wraps the cell cipher's output in a routing record
that names its space epoch. The record mechanism is `jazz.e2ee.cell-record`,
version 1, in the common envelope:

| Field         | Encoding                                                                  |
| ------------- | ------------------------------------------------------------------------- |
| Header        | Common envelope header, mechanism `jazz.e2ee.cell-record`, version 1      |
| Epoch         | Exactly 36 ASCII bytes: the space epoch's lowercase UUID (version 4 or 7) |
| Cipher output | Remaining bytes: the cell cipher's own common envelope (header, payload)  |

The payload (epoch plus cipher output) is at least 47 bytes. The epoch is
routing metadata in clear; it is also authenticated, because the cell
cipher's context names the same epoch. That context is
`frame[C, T]`, where `frame` is u32be-length-prefixed concatenation, `C` is
the canonical context with application = the shared application domain,
policy `jazz.e2ee.cell-record.v1`, scope = the space table's catalogue
identity, identifier = the space identifier, table, row and column = the
catalogue table identity, row UUID and catalogue column identity, epoch = the
epoch UUID and empty recipient, and `T` is the
[cell type](#cell-type-version-1) of the logical column. The plaintext is
Jazz's native row encoding of the single value.

Readers reject a record whose mechanism is another (reported as
`unsupported-format`, which lets a reader try the
[stream record](E2EE_LARGE_VALUE_FORMAT.md#stream-record-version-1)), a payload
under 47 bytes, an epoch that is not a lowercase version 4 or 7 UUID, an epoch other than the
key's, and cipher output that is not an envelope of the installed cell
mechanism (`invalid-ciphertext`, or `unsupported-format` for an unknown
mechanism).

Framing example (not a decryptable value): epoch
`00000000-0000-4000-8000-000000000000` and, as cipher output, the `test`
fixture above:
`4a45324501156a617a7a2e653265652e63656c6c2d7265636f72640000000130303030303030302d303030302d343030302d383030302d3030303030303030303030304a45324501047465737400000001aa`.
This example is pinned by this document and asserted by the `jazz-e2ee`
vector tests.

### Cell type, version 1

`T`, encoding `jazz-e2ee-cell-type-v1`, is UTF-8 JSON without whitespace,
object keys in UTF-16 code unit order:
`{"column_type":<column type>,"nullable":<true|false>}`. A column type is
`{"type":"<name>"}` for `Integer`, `BigInt`, `Boolean`, `Text`, `Timestamp`,
`Double`, `Uuid` and `Bytea`;
`{"type":"Enum","variants":[<variant>, …]}` in declaration order;
`{"schema":<schema>,"type":"Json"}`, or `{"type":"Json"}` without a schema;
and `{"element":<column type>,"type":"Array"}`. Strings are written as
ECMAScript `JSON.stringify` writes them. A JSON schema keeps its values,
objects with sorted keys and integers in decimal; a schema with any other
number cannot be encrypted. Other column types cannot be encrypted.
Fixtures: `{"column_type":{"type":"Text"},"nullable":false}` and
`{"column_type":{"element":{"type":"Enum","variants":["x"]},"type":"Array"},"nullable":false}`,
asserted by the `jazz-e2ee-client` tests.

## Built-in key wrapping, version 1

Mechanism `jazz.wrap.xchacha20poly1305`, version 1, wraps exactly one 32-byte key under a
32-byte symmetric wrapping key. The first payload byte is the operation, 1;
readers reject any other operation byte and any other length. The interface is independent of CellCipher; custom
key envelopes do not change cell encryption or common context semantics.

Associated data is the common envelope header, byte 1, then canonical context.
Derive a 32-byte key with keyed BLAKE2b using the wrapping key and input
`UTF8("jazz.e2ee.wrap-key.v1") || 0x00 || associatedData`. The separator is
one zero byte. The payload is byte 1, a fresh 24-byte nonce, and
XChaCha20-Poly1305-IETF ciphertext with its appended 16-byte tag. Its length
is exactly 73 bytes. This purpose label separates wrapping from cells.

Fixture (generator key `00`–`1f`, the context fixture, nonce `00`–`17`,
wrapped key bytes `20`–`3f`): `wrapDerived` is
`a725b648e755974ef9a69b5acb03be13d561547dc2b4d1d380033b9119d4dc23`,
`wrapAad` is
`4a453245011b6a617a7a2e777261702e786368616368613230706f6c793133303500000001014a45324301000000016100000001700000000173000000016900000001740000000172000000016300000001650000000164`
and the envelope is
`4a453245011b6a617a7a2e777261702e786368616368613230706f6c79313330350000000101000102030405060708090a0b0c0d0e0f10111213141516175f4e9275e4e56d266c7621949e0aae0329cbef65778ca9d4498c602e7e280296eed23822869f2300f7907e08149f0669`.
The `jazz-e2ee` vector tests assert all three. These three values are the
crate's own output after the mechanism rename: `fixtures/e2ee-vectors.c` has
been updated to the new ID but not yet rerun, so they are not independently
generated until the generator is compiled against libsodium and reproduces
them.

## Built-in key sealing (HPKE), version 1

Mechanism `jazz.wrap.hpke-x25519`, version 1, seals exactly one 32-byte key
to a recipient public key (a device, an account epoch or a recovery root) with
HPKE ([RFC 9180](https://www.rfc-editor.org/rfc/rfc9180)) in base mode,
single-shot, suite DHKEM(X25519, HKDF-SHA256) / HKDF-SHA256 /
ChaCha20Poly1305 (KEM `0x0020`, KDF `0x0001`, AEAD `0x0003`). ChaCha20Poly1305
is chosen over AES-GCM because it is constant-time in portable software
(WASM and phones without AES instructions) and matches the rest of the format.

Let `H` be this mechanism's common envelope header with no payload and `C` the
canonical operation context. HPKE `info` is `H`, which binds the mechanism and
version into the key schedule; HPKE `aad` is `C`, the context bytes unchanged.
The plaintext is the 32-byte key. The payload is the 32-byte encapsulated key
`enc` followed by the 48-byte ciphertext and tag, exactly 80 bytes; readers
reject any other length. A small-order recipient key fails sealing. Sealing
authenticates the contents to the recipient, not a sender identity; ordinary
Jazz authorisation remains required. Sealing to a small-order recipient key
fails with `InvalidInput`, not a provider failure.

Recipient key pairs come from HPKE `DeriveKeyPair` (RFC 9180 §7.1.3): a new
device uses 32 fresh random bytes as input keying material, and an account
epoch derives its input keying material from the account epoch key (see the
protocol's account sealing keys). The
public key is the 32-byte X25519 public key; implementations may store the
secret key clamped.

## Fixtures

`fixtures/e2ee-vectors.c` independently constructs the framing and derivation
inputs against libsodium 1.0.22, exactly as the crate's dev-dependency
`libsodium-sys-stable` =1.24.0 bundles and builds it (the generator's header
gives the build command and its `main` checks the version), including HPKE
assembled from libsodium's
X25519, HKDF-SHA256 and ChaCha20-Poly1305. It first reproduces RFC 9180
appendix A.2.1 (sequence 0), then seals the generator's key (bytes `20`–`3f`)
for the context fixture to the recipient derived from input keying material
`2a`–`49`, with ephemeral input keying material `60`–`7f`. The recipient public
key is `46b25ef1bf15f25cc4f52403375de884edb39be47f6871666d3debabfa179b4c` and
the envelope is
`4a45324501156a617a7a2e777261702e68706b652d78323535313900000001bad762e098446cbcc58b30c7e804f82847b8b911185996582af9f27bfd4e875fe4ae0a4407d26ccd2db3ecc953f3f7920e52778aa67b3514be1978fc6b72a56e9952881f202c3001a9af1cf161de2986`.
Every fixture field is deterministic. The `jazz-e2ee` vector tests assert the
RFC vector, this envelope and the other stored vectors, and reject a flipped
bit at every envelope position. This is independent protocol framing, not an
independent security audit. The generator must be compiled with assertions
enabled; `NDEBUG` is rejected.

## Built-in device signatures, version 1

Mechanism `jazz.sodium.sign`, version 1, is single-part detached Ed25519
(RFC 8032), not Ed25519ph, byte-compatible with libsodium's. Let `H` be its
common envelope header with no payload and `R` the protocol's canonical
signed-record bytes (pinned per record in the [protocol](E2EE_PROTOCOL.md)).
Sign exactly `H || R`; store the 64-byte detached signature as the envelope
payload. Verification checks the exact mechanism/version, payload length and
signature. The fixed header domain-separates signatures from other protocol
messages.

Public keys contain 32 bytes. Private keys use the 64-byte seed/public-key
representation. Signing reconstructs the keypair from its seed and rejects an
inconsistent public-key half; it clears that temporary private-key buffer
without modifying the caller's key. New signing keys use independent secure
randomness.

In `jazz-e2ee` this is the `Signer` trait (below): `verify` returns
`Ok(true)` for a valid signature, `Ok(false)` for an invalid signature or
malformed input, and `Err(CryptoFailure)` only for a provider failure; signing
rejects malformed keys. This mechanism authenticates bytes, not signer
eligibility or accepted membership.

## Mechanism agility

Each cryptographic role is a small synchronous trait in `jazz-e2ee`
(`jazz_e2ee::agility`): `CellCipher`, `KeyWrapper`, `KeySealer` (the KEM
role, shaped after HPKE so a hybrid KEM such as X-Wing is a drop-in),
`Signer`, `EqualityTagger` and `StreamCipher`. The built-in mechanisms above
are the default implementations.

- **IDs and versions.** Every envelope names its mechanism ID and version in
  the common header. Built-in IDs live in the reserved `jazz.` namespace;
  custom mechanisms use a reverse-DNS ID of at least two labels, such as
  `com.example.kem`. A new incompatible format is a new version or ID, never
  a reinterpretation of an existing one.
- **Routing.** Readers look the header's ID and version up in a registry of
  compiled-in mechanisms, restricted to the application's per-role
  allow-list (`MechanismAllowList`, default: the built-ins;
  `Registry::allowing`). An unregistered or unlisted mechanism is
  `unsupported-format`, never `invalid-ciphertext`. Retiring a mechanism is
  removing it from the list, and is a breaking change by design: records
  and signed chains that depend on it become unreadable or unverifiable at
  once, with no grace path. Retirement is not the same as migrating the
  selection: rotating and re-encrypting onto a replacement lets a content
  mechanism be retired, but not a signature or key wrapping mechanism used
  in an existing account or space chain, since every later key is
  authenticated back through those immutable links (the protocol's
  mechanism agility rules). Such a mechanism stays allowed while any chain
  uses it.
- **Advertisement.** Public keys carry their mechanism: the advertised form
  is `env(mechanism, publicKey)`. Each identity (a device, a recovery root)
  has exactly one sealing key and one signing key, fixed at creation; a
  recipient that supports a new mechanism creates a new identity for it, and
  an account publishes a sealing key of the new mechanism for its current
  epoch (see the protocol's mechanism agility rules).
- **Selection.** The application configuration selects one mechanism per role
  for writing. A sender seals only to a recipient key of the selected
  mechanism and otherwise fails with "recipient unsupported"; there is no
  silent fallback. A configuration naming an unregistered or unlisted
  mechanism is rejected.
- **Migration.** Ship the mechanism in every client, switch the
  configuration, let recipients create identities of the new mechanism, and
  let the next key rotation seal and wrap the new epoch key with it. Old
  records keep their mechanism and stay readable while it remains
  registered and listed; a migration does not by itself make the old
  mechanism retirable.
- **Errors.** Every role keeps these outcomes apart, and no layer above may
  merge them:
  - `InvalidCiphertext` (an authentication failure, or malformed or truncated
    input to a decryption, unwrap or open) and, for verification, `Ok(false)`;
  - `UnsupportedFormat` (no registered mechanism has the header's ID and
    version);
  - `NoKeyForMechanism` (the mechanism is registered, but the reader holds no
    key pair of it), distinct from an unsupported format;
  - `InvalidInput` (the caller supplied an unusable argument, such as a
    small-order recipient key for sealing);
  - `CryptoFailure` (the provider failed: a hardware-backed key, a host
    callback, randomness). It is never reported as invalid ciphertext or as a
    false verification. `Signer::verify` therefore returns `Result<bool>`, and
    the record helpers (`decrypt_cell_value`, `decrypt_stream_value` and the
    stream update and finish steps) propagate `CryptoFailure` unchanged
    instead of mapping every post-routing error to `InvalidCiphertext`.
- **Compiled in.** Mechanisms are Rust code linked into every binding. There
  are no runtime crypto callbacks from other languages. A host may provide
  the device-key operations of a mechanism (for hardware-backed keys, where
  the secret key bytes are a key reference), still behind the same traits.

A mechanism is accepted once it passes the conformance kit
(`jazz_e2ee::conformance`, feature `conformance`): round trips, key and
context binding, rejection of tampered, truncated and extended input
(streams also of reordered, duplicated and dropped segments), routing by its
own header, authentication of its header (`check_*_header_binding`: the kit
derives two siblings of the mechanism under test itself, its ID with the
last character changed and its version changed by one, and builds each
with the author's relabelling constructor, after checking that relabelling
to the mechanism's own ID and version reproduces the implementation byte
for byte; output relabelled to a sibling must decode and then fail
authentication), exact reproduction of the vectors
it declares, and, per role,
provider failure: a mechanism whose provider fails must surface
`CryptoFailure` through the registry and the record helpers, never
`InvalidCiphertext` or a false verification. The crate's
`custom_mechanism` example registers, checks and selects a toy KEM.
