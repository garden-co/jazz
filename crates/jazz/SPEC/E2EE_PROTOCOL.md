# E2EE protocol

This companion specifies the end-to-end encryption protocol: the identities,
the immutable records that carry keys, how a client replays them, and how
reads, writes and private local state use the resulting keys. It is the
reference for a reimplementation and for its review. The byte formats
underneath are specified separately: [cryptographic formats](E2EE_CRYPTO_FORMAT.md)
(context, envelopes, cell cipher, key envelope, signatures),
[equality tokens](E2EE_EQUALITY_FORMAT.md) and
[large values](E2EE_LARGE_VALUE_FORMAT.md).
Schema changes that involve encrypted data are specified in [E2EE migrations](E2EE_MIGRATIONS.md).

**The model in one paragraph.** A **space** is one application row (for
example a `projects` row) together with the encrypted rows that name it (its
`tasks`). Whoever Jazz lets read the space row may decrypt everything in the
space; nothing else decides. Each space has a chain of key versions, each
linking back to the previous one, so the newest key opens the whole history.
A client that lacks a key writes a key request, and any reader holding the key
answers it by sealing the key to the requester's account. Only sessions that
may update the space row create the space or rotate its key. Encryption keeps
content unreadable outside Jazz's permissions; it adds no access rule of its
own (§8).

The account and device lifecycle was derived from an earlier TypeScript
implementation and its tests, then amended by design decisions; where the two
differ, this document notes it briefly. The space model replaces that
implementation's space and membership design entirely. Points that remain
undecided are marked **Open question**.

**Precedence.** This document governs protocol behaviour (records, replay,
permissions, reads, writes, local state and failure codes). The three format
specs govern bytes and the mechanism interfaces of `crates/jazz-e2ee`, which is
the normative reference implementation of the formats. [E2EE migrations](E2EE_MIGRATIONS.md)
extends this protocol for schema conversions; every code and read rule it adds
is also stated here (§4.1, §4.3, §5.1, §7), and where the two still differ this
document wins. The [core hooks note](E2EE_CORE_HOOKS.md) proposes how jazz-core
provides what this document requires and does not change it. A reviewed
specification is not an implementation or platform qualification: byte
fixtures and behavioural tests must accompany each part when it is built.

## 0. Conventions

### Notation

| Notation          | Meaning                                                                                                                                       |
| ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `ctx{…}`          | A canonical context, version 1 (`JE2C 01`, nine u32be-length-prefixed UTF-8 fields). Omitted fields are empty.                                |
| `frame[a, b, …]`  | Concatenation of `u32be(len(x)) ‖ x` for each part.                                                                                           |
| `C ‖ frame[…]`    | A context followed by framed fields (the "signing bytes" of most lifecycle records).                                                          |
| `env(m, p)`       | Common envelope `JE2E 01`, mechanism `m` (id, version), payload `p`.                                                                          |
| `json(x)`         | ECMAScript `JSON.stringify(x)` as UTF-8: no whitespace. Arrays of strings and nested arrays only, unless stated.                              |
| `sorted`          | Ascending by UTF-16 code unit (identical to byte order for the ASCII IDs used here).                                                          |
| `Z32`             | 32 zero bytes.                                                                                                                                |
| `seal(pub, C, k)` | HPKE key sealing (`jazz.wrap.hpke-x25519` v1) of a 32-byte key `k` to a public key, with context bytes `C` as HPKE `aad`. `open` reverses it. |
| `wrap(K, C, p)`   | Symmetric key wrapping (`jazz.wrap.xchacha20poly1305` v1, operation 1) of 32 bytes `p` under key `K`. `unwrap` reverses it.                   |
| marker            | `wrap(K, C, Z32)`. A marker is valid for `K` iff `unwrap` succeeds and yields exactly `Z32`.                                                  |
| `sign`, `verify`  | Device signature mechanism (`jazz.sodium.sign` v1) over the given bytes.                                                                      |

The context argument of `seal`/`wrap` is arbitrary bytes; most are a `ctx{…}`,
some are `ctx{…} ‖ frame[…]`.

### Cryptographic primitives

The primitives (XChaCha20-Poly1305, Ed25519, keyed BLAKE2b, SHA-256) are
implemented in pure Rust on every target, byte-compatible with libsodium; the
vectors of the format specs pin that compatibility. Keys are sealed to public
keys with HPKE (RFC 9180, DHKEM(X25519, HKDF-SHA256), HKDF-SHA256,
ChaCha20Poly1305; see the crypto format), and large values use RustCrypto's
STREAM construction over XChaCha20-Poly1305 (see the large-value format). A
crypto provider failure (as opposed to a provable authentication failure)
fails the operation loudly; it is never reported as an unavailable key or an
invalid proof. The mechanism interfaces therefore distinguish the two: a
verification returns "valid", "invalid" or a provider failure, and a decryption
or unwrap reports a provider failure separately from an authentication failure
(see the crypto format's mechanism agility rules).

### Mechanism agility

Every role (content cipher, key wrapping, key sealing, signatures, equality
tokens, large values) can change mechanism without a format break; the rules
are in [the crypto format](E2EE_CRYPTO_FORMAT.md#mechanism-agility). For this
protocol:

- Every published public key (device, account sealing key, recovery root)
  carries its mechanism ID and version (the `mechanism`/`version` and
  `signingMechanism`/`signingVersion` columns, or `env(mechanism, publicKey)`
  in signed bytes). A device request and a recovery root each have exactly
  one sealing key and one signing key, fixed when they are created, and
  never gain a second mechanism. Supporting a new mechanism means: a device
  enrols a new device ID (approved as in §2.5) and the old one is removed
  (§2.8); a recovery secret derives a second root for the new mechanism
  (§2.9); an account publishes a sealing key of the new mechanism for its
  current epoch (§2.12); a space adopts it at its next rotation (§3.5).
- Writers seal, sign and encrypt with the mechanism the application's
  configuration selects for the role. Sealing to a recipient that advertises
  no key of the selected mechanism fails with "recipient unsupported"; there
  is no fallback to another mechanism.
- Readers route by each envelope's mechanism and accept a registered
  mechanism only if the application's **allow-list** names it for that role
  (`encryption.mechanisms.allowed` in `jazz.app.json`, per role; default:
  the built-ins). The client runtime reads the key and builds the reader's
  registry from it (`Registry::allowing` in `crates/jazz-e2ee`); the crate
  itself reads no configuration. An envelope of any other mechanism is
  `unsupported-format` (§4.3, §7), never decrypted, opened or verified, even
  if its implementation is compiled in. Records written under an earlier
  selection stay readable while their mechanism stays listed.
- **Retiring a mechanism is a breaking change, by design.** Retiring (for
  example after a weakness is found) is removing the mechanism from the
  allow-list, and it takes effect at once: every record that depends on it
  becomes unreadable (`unsupported-format`) and every signed chain that
  depends on it unverifiable (an unsupported capability, §2.4 step 4, §7),
  for every reader of the application. There is no grace period, fallback
  or partial acceptance. Before retiring, the developer migrates: switches
  the selection (below), lets every account publish sealing keys and every
  space rotate onto the new mechanism, re-encrypts content that must stay
  readable, and only then removes the old mechanism. Writers can select only
  listed mechanisms.
- **No delivery can be downgraded.** Every delivered key is bound by its
  sender to the bytes of its envelope, or authenticated before use by a
  marker only a holder of the key can make, so nobody without the key (the
  server included) can substitute an envelope sealed under another
  mechanism: account successor deliveries are covered by the successor's
  signature (§2.8); approval deliveries by the delivery-verification marker
  over the envelope (§2.5); identity envelopes, account recovery deliveries,
  space envelopes and space deliveries yield a key that is accepted only
  after it authenticates its epoch's marker and chain (§2.7, §3.3), which
  signed records carry. A key holder could seal under any listed mechanism,
  but it could also disclose the key.
- Migration: ship the new mechanism to every client, then switch the
  configuration. Each client then enrols a new device identity of the new
  mechanism and removes its old one; recovery secrets register their second
  root; each account publishes its sealing key of the new mechanism; the next
  rotation of each space wraps its new key with it. Until an account has a
  sealing key of the selected mechanism, sealing to it fails with "recipient
  unsupported" and its key requests wait. Old records are not rewritten.

### Identifiers

Every record ID, epoch ID, device ID and recovery root ID is a lowercase UUID
string, generated at random, except where a derivation is stated. Signed
statements check IDs against
`^[0-9a-f]{8}-[0-9a-f]{4}-[47][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`
(versions 4 or 7). Account IDs are the authenticated Jazz account ID: any
non-empty string; a local-first account's ID is derived from its
local-first public key (§2.3). A recovery root derived from a recovery
secret (§2.9) has a derived version 8 ID, and recovery root IDs accept
versions 4, 7 and 8. Reset identities (§1.2) and key requests (§3.4) have
derived version 8 IDs and accept version 8 only.

Version 8 IDs come in two families:

- **Public derived IDs** (reset identities, key requests). Each is the first
  16 bytes of `SHA-256(UTF8(json([label, …])))`, with
  `b[6] = (b[6] & 0x0f) | 0x80` and `b[8] = (b[8] & 0x3f) | 0x80`, as a
  lowercase UUID; integers in the JSON (an `attempt`, a `generation`) are
  decimal strings. The server checks a reset identity's ID (R5). It does not
  check key request IDs: clients do, and ignore a request whose ID is not its
  own derivation (§3.4). Row IDs are unique per table, so a reader of a space
  can occupy another account's next request ID with a row of its own; the
  requester steps to its next attempt (§3.4, §8).
- **Secret-derived recovery root IDs** (§2.9): keyed BLAKE2b from the
  recovery seed (which binds the account generation above 0), with the same
  version and variant bits. The server cannot recompute them; it checks only
  the version 8 form, R9, and that the ID is not already used. Only the
  account itself may register a root (R9), so a wrong ID can only break that
  account's own recovery, which the client detects by deriving again.

Fixtures (`crates/jazz-e2ee/tests/protocol_vectors.rs` pins each label,
cross-checked against libsodium's SHA-256):

| JSON input                                                                                                                                                    | Derived ID                             |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------- |
| `["jazz.e2ee.account-reset.v1","alice","1"]`                                                                                                                  | `0499408f-e71b-82e6-b00e-5cd5a54871e4` |
| `["jazz.e2ee.key-request.v1","5c0e1d2a-3b4c-7d5e-8f60-718293a4b5c6","018f2e3d-4c5b-7a69-8877-665544332211","bob","7f3c1a2b-4d5e-4f60-8a7b-9c0d1e2f3a4b","0"]` | `459a4f23-f61e-8e17-bfc9-f10135b08e53` |
| the same with attempt `"1"`                                                                                                                                   | `5607af4c-b861-8b22-9164-ad75a00bc9c9` |

Secret-derived recovery root fixtures, for generations 0 and above, are in
§2.9; the account sealing key fixture is in §2.12. The same test file pins
the recovery derivation and the SHA-256 digest of every record kind's signed
bytes, each rebuilt there from this document's formulas; those digests pin
this document's reading, not an independent implementation's. The
key-request and account sealing key fixtures above are computed from this
document and are to be added to that file. A row that no derivation covers
must use a version 4 or 7 ID. An insert refused because a row with that ID
already exists is reported to the writer exactly like a policy refusal, so a
refusal does not reveal whether a derived row exists.

Canonical JSON fields of account successors (`membership`, `revision`,
`deliveries`) are parsed and re-encoded; a field whose re-encoding is not
byte-identical is rejected.

- **ID list** (`encodeEpochIds`): `json(sorted ids)`, strictly increasing, all
  UUIDs. A **revision** uses the same encoding but allows non-UUID strings.
- **Delivery map** (`encodeEpochDeliveries`): `json([[deviceId, [byte, …]], …])`
  sorted by device ID, each envelope 1–65,536 bytes.

### Application domains

The `application` field of every context is one of two domains:

- **Shared domain** `Dₛ = json([appId, env])`.
- **Account domain** `Dₐ(account) = json([appId, env, account])`.

`appId` is the application's canonical lowercase UUID, taken from the
application's configuration (its app files); `account` is the authenticated
account. No URL or registry path is parsed. (The TypeScript implementation read
the app ID from the account registry path `<server>/apps/<appId>/accounts`.)
`env` is the configured environment label
and defaults to `"dev"` (a client warns once when a production process leaves
it unset). A configured empty label is refused, not read as unset. The server URL is not part of either domain, so data survives a move
to another host.

| Records                                                                                        | Domain                            |
| ---------------------------------------------------------------------------------------------- | --------------------------------- |
| Content: cells, equality tokens, streams                                                       | `Dₛ`                              |
| Account ledger records of account `a` (devices, approvals, successors, recovery, sealing keys) | `Dₐ(a)`                           |
| Space records (root, successors, deliveries)                                                   | `Dₐ(space root creatorAccountId)` |
| This device's private store scope                                                              | `Dₐ(self)`                        |

A different author, sender or recipient never changes a space's domain: it is
always its creator's.

### What Jazz provides

The protocol is a client-side state machine over ordinary Jazz rows. It relies
on Jazz for:

1. **Authenticated authorship.** Every row's `$createdBy.account` is the
   authenticated account that wrote it, and policies can compare it.
2. **Policy enforcement** of the requirements in §1.4, at the authority,
   including policies that reach a space row from an internal row through
   `InheritsFrom` ([core hooks](E2EE_CORE_HOOKS.md) H7).
3. **Ledger reads.** Inside a serializable transaction, a read of immutable
   rows returns, for each row, its accepted position (an unsigned 64-bit
   integer, decimal string at the boundary) and a common cut `through`. All
   reads of one evidence set must report the same cut; a client refuses
   evidence that spans cuts. Every returned row has exactly one position and no
   position exceeds the cut.
4. **Ledger transactions.** Inserts staged in one transaction become accepted
   atomically, at one position. Account operations, space creation and
   rotation wait for Global acceptance (the authority's decision), not only
   Local.
5. **Read-set validation.** The authority refuses to commit a ledger
   transaction (with a conflict, not a policy refusal) when any evidence the
   transaction read at its cut has changed by its commit: a row it read was
   superseded in a way the read would have returned, or a row matching one of
   its reads was accepted after the cut. The protocol relies on this for one
   accepted bootstrap or reset per account and generation (§2.3, §2.11), one
   accepted successor per predecessor (§2.4, §3.5) and one root per space
   (§3.2).
6. **Provisional reads** (offline). A snapshot `{owner, acceptedThrough, own}`
   whose rows carry either an accepted position or an own pending transaction
   of this writer. Accepted rows sort before own pending rows; pending rows
   sort by transaction coordinate. A pending coordinate is never treated as an
   accepted position.

**Accepted order.** Replays visit rows by accepted position. Rows with equal
position were accepted in one transaction; the tie rules are stated per
replay. "Before `p`" always means strictly earlier position.

## 1. Model and terms

### 1.1 Terms

- **Account.** A Jazz account, authenticated by the application's normal
  sign-in. Signing in establishes the account identity only; it never approves
  a device. An account has no device-independent keypair of its own; its
  sealing keys derive from its epoch keys (§2.12).
- **Device.** One installation's encryption identity: a key-sealing keypair
  (HPKE X25519) and a signing keypair (Ed25519), plus a UUID. A device
  belongs to one account domain on one install.
- **Account epoch and epoch key.** An account's key history is a chain of
  epochs. Each epoch has a UUID and a random 32-byte **account epoch key**,
  delivered to every active device of that epoch. The first epoch is the
  **account root**; each later epoch is an **account successor**. An
  **account reset** (§2.11) starts a new chain, of the next **generation**,
  that replaces the earlier one.
- **Active device.** A device admitted to the current account epoch by the
  root, an approval, or a successor's membership, and not removed.
- **Account sealing key.** A key pair derived from an account epoch key,
  whose public half the account publishes (§2.12). Space keys are sealed to
  it, so every device holding the account's key opens them.
- **Recovery root (recovery credential).** A key-free public record naming a
  recovery keypair (envelope + signing). The private half is derived from a
  **recovery secret** (§2.9): for a local-first account, the account secret
  itself; for an externally authenticated account, a separate secret backed up
  in a passkey. Exported **recovery material** holding the keypair directly is
  the fallback. A recovery root can receive the account epoch key and
  authorise a new device.
- **Space.** The encryption scope of rows: one application row, the **space
  row** (for example a `projects` row), and the encrypted rows that name it
  (its `tasks`). A space is named by its address `(scopeId, identifier)`:
  `scopeId` is Jazz's stable catalogue identity of the space row's table (the
  **space table**), `identifier` is the space row's ID. Every encrypted row
  of a space is encrypted under the space's keys.
- **Reader, updater.** A session that Jazz lets read (update) the space row.
  Readers may obtain the space's keys; updaters may create the space and
  rotate its key. Jazz permissions alone decide both.
- **Key version.** A space's key history is a chain: the root version, then
  one per **rotation** (a successor). Each version has an epoch ID and a
  random 32-byte key, and wraps the previous version's key, so the newest key
  opens every earlier one.
- **Key request and delivery.** A key request asks for one key version for
  one account; a delivery answers it with that version sealed to the
  account's sealing key.
- **Held key.** A space key this device has authenticated and stores in its
  private store (§6). A held key is sufficient to read and write.

### 1.2 Account and device tables

All E2EE tables (this section and §1.3) are internal Jazz tables, like Jazz's
own account tables: they never appear in the application's `jazz.app.json` or
`jazz.permissions.json`, and the application cannot define or override their
policies. The tables in this section have fixed policies (§1.4). "Public" means readable by any session in `local-first` or
`external` auth mode; "private" means readable only by the row's
`$createdBy.account`. Every row is immutable.

**Column types** (this section and §1.3). IDs, account IDs, mechanism IDs
and the enumerated columns are strings; keys, envelopes, markers, signatures,
and canonical JSON columns (`membership`, `revision`, `deliveries`) are
bytes; `version`, `signingVersion`, `ledgerVersion` and `attempt` are
unsigned 32-bit integers (the common envelope's version width);
`generation` is an unsigned 64-bit integer, written as a decimal string
wherever it enters JSON. Optional columns
(`recoveryRootId?`, …) are nullable.

| Table                              | Columns                                                                                                                                                                                   | Row ID          | Visibility | Meaning and writer                                                                                      |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------- | ---------- | ------------------------------------------------------------------------------------------------------- |
| `__e2ee_device_requests`           | `publicKey`, `mechanism`, `version`, `signingPublicKey`, `signingMechanism`, `signingVersion`                                                                                             | device ID       | private    | A device's identity, written by that device.                                                            |
| `__e2ee_device_keys`               | `deviceId` and the six key columns of the request                                                                                                                                         | random          | public     | Public projection of the request's keys; must equal it. Written by the device.                          |
| `__e2ee_account_identities`        | `ledgerVersion` (1), `generation`, `deviceId`, `epochId`, `envelope`, `verification`                                                                                                      | identity ID     | private    | Private account root: root epoch key sealed to the first device, plus its marker. Written at bootstrap. |
| `__e2ee_account_roots`             | `ledgerVersion` (1), `generation`, `accountId`, `deviceId`, `epochId`, `kind`, `localFirstPublicKey?`, `localFirstSignature?`                                                             | random          | public     | Public account root. Written at bootstrap in the same transaction as the identity.                      |
| `__e2ee_device_challenges`         | `deviceId`, `epochId`, `envelope`                                                                                                                                                         | challenge ID    | private    | An approval challenge to a requesting device. Written by the approving device.                          |
| `__e2ee_device_proofs`             | `challengeId`, `proof`, `signature`                                                                                                                                                       | challenge ID    | private    | The requesting device's possession proof.                                                               |
| `__e2ee_device_approvals`          | `challengeId`, `signerId`, `verification`, `signature`                                                                                                                                    | challenge ID    | private    | Private approval with its account-key marker.                                                           |
| `__e2ee_device_deliveries`         | `challengeId`, `envelope`, `verification`                                                                                                                                                 | challenge ID    | private    | Account epoch key sealed to the approved device.                                                        |
| `__e2ee_public_device_approvals`   | `accountId`, `epochId`, `deviceId`, `signerId`, `signature`, `recoveryRootId?`, `recoverySignature?`                                                                                      | challenge ID    | public     | Key-free signed approval: admits the device.                                                            |
| `__e2ee_account_successors`        | `accountId`, `predecessor`, `epochId`, `signerId`, `action`, `removedDeviceId?`, `retiredRecoveryRootIds`, `membership`, `revision`, `verification`, `history`, `deliveries`, `signature` | random          | private    | Private account rotation with the new key's deliveries.                                                 |
| `__e2ee_public_account_successors` | the private columns minus `verification`, `history`, `deliveries`                                                                                                                         | same as private | public     | Key-free signed rotation: the membership change.                                                        |
| `__e2ee_recovery_roots`            | `accountId`, `signerId`, `epochId`, `publicKey`, `mechanism`, `version`, `signingPublicKey`, `signingMechanism`, `signingVersion`, `signature`                                            | root ID         | public     | A registered recovery credential, signed by an active device.                                           |
| `__e2ee_recovery_deliveries`       | `rootId`, `epochId`, `envelope`                                                                                                                                                           | random          | private    | Account epoch key sealed to a recovery root.                                                            |
| `__e2ee_account_keys`              | `accountId`, `epochId`, `signerId`, `mechanism`, `version`, `publicKey`, `signature`                                                                                                      | random          | public     | An account epoch's sealing public key (§2.12), signed by an active device.                              |

`action` is `remove-device` or `retire-recovery-root`; the matching target
column is set. `retiredRecoveryRootIds` is an ID list (§0) of recovery root
IDs, versions 4, 7 or 8. A `retire-recovery-root` successor leaves
`removedDeviceId` unset and lists exactly one root, its target; a
`remove-device` successor sets `removedDeviceId` and lists every still
active root the removed device held the key pair of (§2.8), possibly none
(`[]`).

A root's `kind` is `local-first` (an account founded by a local-first
identity, §2.3) or `app` (an account of the application's own sign-in). A
`local-first` root sets both `localFirstPublicKey` (the raw 32-byte Ed25519
public key) and `localFirstSignature`; an `app` root sets neither. Any other
combination is malformed.

The identity ID is the account ID for generation 0 (the first bootstrap). For
a reset (§2.11) it is the version 8 UUID derived (§0) from
`SHA-256(UTF8(json(["jazz.e2ee.account-reset.v1", accountId, generation])))`,
with `generation` as a decimal string.

The TypeScript implementation also declared `__e2ee_recovery_protectors` and a
32-byte request `challenge` column; neither was ever read, and both are dropped.

### 1.3 Space tables

These tables are internal too. One set serves every space table of the
application. Their policies are derived by the server at deploy time from the
application's encryption declarations (§1.4); the application writes no
policy for them. Every row carries the space's address, `scopeId` and
`identifier`, through which its policies reach the space row.

| Table                     | Columns                                                                                                                                                            | Row ID           | Meaning and writer                                                                          |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------- | ------------------------------------------------------------------------------------------- |
| `__e2ee_spaces`           | `scopeId`, `identifier`, `creatorAccountId`, `deviceId`, `accountEpochId`, `epochId`, `mechanism`, `version`, `verification`, `accountEnvelope`, `signature`       | random           | Space root: the first key version. Written by an updater (§3.2).                            |
| `__e2ee_space_successors` | `scopeId`, `identifier`, `predecessor`, `epochId`, `authorAccountId`, `authorDeviceId`, `authorEpochId`, `verification`, `history`, `accountEnvelope`, `signature` | random           | A rotation: the next key version, linked to the previous one. Written by an updater (§3.5). |
| `__e2ee_key_requests`     | `scopeId`, `identifier`, `keyEpochId`, `accountId`, `attempt`                                                                                                      | derived (§3.1)   | An account's request for a key version. Written by a reader for itself (§3.4).              |
| `__e2ee_space_deliveries` | `scopeId`, `identifier`, `keyEpochId`, `senderAccountId`, `recipientAccountId`, `recipientAccountEpochId`, `envelope`                                              | its request's ID | The answer to one request: the key version sealed to the requester's account (§3.4).        |

Each encrypted application table stores every encrypted column as non-null
`bytes` (ciphertext; a null value is encrypted as a null value), plus a
non-null `bytes` column `__e2ee_eq_<column>` for each equality-indexed column.
Two short values are never ciphertext and have fixed meanings (§4.1): the
empty byte string (never written) and the 5-byte cleared marker
`4a 45 32 58 01` (written by a schema conversion, [migrations](E2EE_MIGRATIONS.md) §3.2).

### 1.4 Permissions

Jazz is the only permission authority. E2EE adds no permission logic of its
own: replay authenticates signatures, device activity, chains and IDs, but it
does not decide who may read or update a space, and possession of a key
grants no Jazz permission. The server enforces access only, with policies;
it checks no signature, key chain or derived ID of a space record (clients
do, §3.3–§3.4). The policies below are what Jazz enforces on the internal
tables.

All E2EE tables:

- R1. No update and no delete, ever.
- R2. Every row's author, sender or creator column (`accountId`,
  `creatorAccountId`, `authorAccountId`, `senderAccountId` as applicable)
  equals the session account and `$createdBy.account`.

Account and device tables (fixed policies):

- R3. `__e2ee_device_requests`: insert by any account as `$createdBy`; read by
  creator only.
- R4. `__e2ee_device_keys`: insert only when the request whose row ID equals
  this row's `deviceId`, created by the same account, has identical key and
  mechanism columns.
- R5. `__e2ee_account_identities`: insert only with the identity ID of the
  session account and the row's `generation` (§1.2) and a request for
  `deviceId` created by the same account; generation 0 only when the account
  has no identity, a later generation only when the account has an identity
  of the generation before it, the session is not `local-first`, the account
  was not founded by a local-first identity, and the deployed encryption
  configuration sets `allowAccountReset` (§2.11); read by that account.
- R6. `__e2ee_account_roots`: insert only with `accountId = session account`
  and a matching identity (identity ID of that generation, same device, epoch,
  generation and ledger version) by the same account, in the same
  transaction. The root's `kind` is `local-first` exactly when the account
  was founded by a local-first identity (Jazz's account registry), and the
  server then also checks the local-first key and signature as replay does
  (§2.4 step 1); an account founded otherwise writes `app` roots.
- R7. Challenges, proofs, approvals, device deliveries: insert by `$createdBy`
  only, referencing a request or challenge of the same account; read by
  creator.
- R8. Public device approvals: `accountId = session account`; the row ID is
  the ID of a private approval of the same account inserted in the same
  transaction; the signer's
  request and the approved device's request belong to that account; a
  recovery approval's `recoveryRootId` names a recovery root of that account.
  Both successor tables: `accountId = session account`; the signer request
  and the target (removed device's request or retired recovery root) belong
  to that account; the target columns match `action` (§1.2); the
  private and public rows share their ID and are inserted in one
  transaction.
- R9. Recovery roots: `accountId = session account`, signer request of the
  same account. Recovery deliveries: the root's `accountId` is the session
  account; read by creator.
- R10. Public tables (roots, device keys, public approvals, public successors,
  recovery roots, account keys) readable by every `local-first` or `external`
  session, so other accounts can replay an account's history.
- R11. `__e2ee_account_keys`: insert only with `accountId = session account`
  and a signer request of the same account; readable like the other public
  tables (R10).

Space tables (policies derived at deploy). Each internal space row names a
space row by `(scopeId, identifier)`. Its policies reach that row through
`InheritsFrom` ([core hooks](E2EE_CORE_HOOKS.md) H7): `scopeId` must be the
catalogue identity of a space table declared at deploy, and the row of that
table whose ID is `identifier` must pass that table's own policy for the
operation. A row naming any other table passes nothing. Below, **read(row)**
and **update(row)** mean exactly this check for the session.

- R12. **Reads.** The root, successors and key requests of a space are
  readable by sessions that pass read(row). A delivery is readable only by
  its recipient account (`recipientAccountId` = session account).
- R13. **Create.** Insert into `__e2ee_spaces` iff update(row), evaluated on
  the state the transaction leaves (a space created with its row is checked
  against the new row), and no root of the same address is accepted.
- R14. **Rotate.** Insert into `__e2ee_space_successors` iff update(row).
- R15. **Request.** Insert into `__e2ee_key_requests` iff read(row) and
  `accountId` = session account.
- R16. **Deliver.** Insert into `__e2ee_space_deliveries` iff read(row) and
  the row's ID is the ID of an accepted key request with the same
  `scopeId`, `identifier` and `keyEpochId` whose `accountId` is
  `recipientAccountId`. Row IDs are unique, so a request has at most one
  accepted answer.
- R17. Application tables keep their own read and write policies. The space
  row's read rule is the confidentiality rule of everything in the space
  (§1.5, §8).

Nothing else is checked by the server: not that a successor's author holds
the key, that a request's ID is its derivation, or that an envelope opens.
Clients verify those, and ignore a row that fails (§3.3–§3.4). A read rule
that uses session claims works unchanged: a request is checked against the
requester's own session. (The TypeScript implementation declared the space
tables in the application schema with hand-written policies, and supported
one space table per application.)

### 1.5 App-file declarations

An encrypted table's `encryption` entry names its space: `space` is the
column holding the space row's ID (`id` for a table whose rows are their own
spaces), and `scope` is the space table. Nothing else is declared: who may
read and update the space row, in the application's ordinary permissions,
decides who may decrypt, create and rotate.

```json
"projects": { "columns": { … } },
"tasks": {
  "columns": { "projectId": "uuid", … },
  "relations": { "project": { "rel": "projects", "column": "projectId" } },
  "encryption": {
    "space": "projectId", "scope": "projects",
    "columns": { "title": "string", "notes": "string" }, "equality": ["title"]
  }
}
```

- **Column entries.** A `columns` value is either a type string (`"title":
"string"`) or, during a schema conversion, an object with `type` and the
  conversion fields of [migrations](E2EE_MIGRATIONS.md) §3.2 (`from`, `to`,
  `convert`, `sealedType`); the string is shorthand for `{ "type": … }`.
- The deploy and code generation refuse a `scope` that is not a table with an
  `id`, and a `space` column that is not `id` (with `scope` the table itself)
  or a `uuid` column referencing `scope`.
- **Entitlement is read access to the space row.** Finer rules inside a space
  (a private task in a shared project) are not cryptographically separated:
  every reader of the project can decrypt the private task. Content that
  needs a narrower readership gets its own space: documentation recommends a
  dedicated content row (for example a `privateNotes` row per owner) whose
  read rule is exactly the intended readership. The deploy warns when it can
  see that the space row is readable more broadly than the encrypted table
  (for example, the table's read rule is the space row's rule plus further
  conditions), and warns when a space table's read rule admits every
  session. It cannot decide the comparison in general and does not claim to.

Application-wide encryption settings live in an optional top-level
`encryption` object of `jazz.app.json`, beside `tables`: `allowAccountReset`
(boolean, default `false`, §2.11) and `mechanisms` (selection and
allow-list, §0). Like the per-table entries, it is client and deploy
metadata, outside the schema hash, carried in the deploy request:

```json
{ "encryption": { "allowAccountReset": true }, "tables": { … } }
```

## 2. Account and device lifecycle

All operations in this section run as one ledger transaction each (a
**publication**) at a named stage, and fail with a publication error
(`refused` or `unknown`, §7), which keeps the specific failure code when there
is one. Contexts use the subject account's domain
`D = Dₐ(account)`.

### 2.1 Local device

A device generates, on first use for an account domain, an envelope keypair,
a signing keypair and a UUID. Before persisting
it checks the pair by a self seal/open and a sign/verify over
`UTF8("jazz.e2ee.local-device-check.v1\0" ‖ scope)`. It is persisted locally
(§6.1, private keys wrapped by a platform key) before anything is published; when several writers race, the store keeps
one device per scope. Loading a stored device requires its original mechanisms.

### 2.2 Device request

Stage `request`. Insert the request row (ID = device ID) and a device-keys row
if absent. An existing row whose keys differ refuses ("immutable device
identity differs"). A request is not an approval.

### 2.3 Bootstrap of the first device

Stage `bootstrap`. Preconditions, all read at one cut:

- the account has no public roots, public approvals, public successors,
  recovery roots, identities, challenges, proofs, approvals, device deliveries
  or private successors; any such evidence refuses;
- exactly one request for this device (otherwise `no-device-request`), with
  at least one key projection, whose keys match the local device (otherwise
  `device-keys-mismatch`).

Then generate `epochId` and a 32-byte key `K`, and insert atomically:

- identity `{id: account, generation: 0, deviceId, epochId, ledgerVersion: 1,
envelope: seal(device.pub, A(deviceId, "envelope"), K),
verification: wrap(K, A("", "verification"), Z32)}`,
- public root `{accountId, generation: 0, deviceId, epochId, ledgerVersion: 1,
kind, localFirstPublicKey?, localFirstSignature?}`,
- the account sealing key record of `epochId` (§2.12),

where `A(recipient, column) = ctx{application: D, policy:
"jazz.e2ee.account-identity.v1", scope: "account", identifier: account,
table: "__e2ee_account_identities", row: account, column, epoch: epochId,
recipient}`. Both envelopes are opened again before submission. At most one
bootstrap is accepted (§0 item 5).

**How a root is authenticated depends on how the account signs in.**

- **Local-first accounts** (`packages/jazz-tools/src/local-first-auth`). The
  account secret derives an Ed25519 key (`SHA-512(UTF8("jazz-auth-sign-v1")
  ‖ secret)[0..32]` as the seed), whose public key `pk` derives the
  **subject** `UUIDv5(namespace = UTF8("jazz-auth-key-v1"), name = pk)` (the
  16 ASCII bytes are the namespace), which Jazz's account registry turns into
  the account ID `UUIDv5(namespace = appId, name =
UTF8("jazz-account-founder-v1") ‖ 0x00 ‖ UTF8(subject))`. The root has
  `kind: "local-first"`, `localFirstPublicKey: pk`, and `localFirstSignature`,
  a `jazz.sodium.sign` v1 signature by that key over

  `ctx{application: D, policy: "jazz.e2ee.local-first-account-root.v1",
scope: "account", identifier: account, table: "__e2ee_account_roots", row:
rootId, column: "local-first-signature", epoch: epochId, recipient:
deviceId} ‖ frame[decimal generation, decimal ledgerVersion, pk,
env(signingMechanism, signingPublicKey), env(mechanism, publicKey)]`

  with the root device's published key columns: the signature binds
  everything the root introduces (account, generation, epoch, device and its
  keys). Writing it needs the account secret; the SDK holds the derived key
  pair only in memory, never prints it, and refuses
  (`local-first-identity-mismatch`) an identity whose key does not derive
  the account ID. Nobody without the secret, the server included, can write
  or substitute such a root.
  Fixture: the secret of bytes `00`–`1f` gives `pk =
ff3d97bfb89227b2ae43697296031fe63bcfb9628f17df15e0e28013021451d9`, subject
  `fa65957a-8b63-53c8-8187-fb1568365fa1` and, for application
  `6b0c3f8e-2d9a-4f61-9a57-1c2e3d4f5a6b`, account ID
  `d31c2893-e3f2-5285-9034-66bb6757657c` (the cases of
  `local-first-auth/test-vectors.json`, checked by both suites).

- **App accounts** (the application's own sign-in). The client has nothing
  to check a root against: the root has `kind: "app"`, is not signed, and its
  authority is `$createdBy` plus R5–R6. Clients trust the first root they see
  (trust on first use, §8). The account's own devices detect a later
  substitution (`root-not-mine`, §2.4).

### 2.4 Account replay

Replay of an account (`replayOrderedAccount`) is over public rows at one cut:

1. **Root.** Any root with `ledgerVersion ≠ 1` is an unsupported capability,
   valid or not. Every other root is checked by itself, against the whole
   read (so every view of one read, current or as of a position, agrees),
   and does not count, and is reported with its code, if:
   - it is malformed (`root-malformed`) or names another account
     (`root-foreign-account`);
   - it is `local-first` and its public key does not derive the account ID
     (`root-key-mismatch`), its device has no published keys accepted at or
     before the root (`root-device-keys-missing`), or its signature over the
     bytes of §2.3, with those keys, does not verify
     (`root-signature-invalid`). Anyone can copy an account's public key into
     a row naming a device without keys; such a row is rejected like any
     other forgery and never stops replay;
   - it is `app` and the read has a valid `local-first` root of the account
     (`root-kind-mismatch`): a local-first account's roots of every
     generation are signed by its identity, so its kind cannot be
     downgraded, and replay accepts a later generation of it only if the
     account's local-first key signed it (§2.11).

   Every per-row check is decided before any contradiction: only a valid
   root can be one.

   The roots that pass form a chain. Generation 0's **counting roots** are
   its valid roots; generation `g > 0`'s are its valid roots accepted
   strictly after the canonical root of `g − 1`. The **canonical root** of a
   generation is its earliest counting root, ties by smallest ID. A valid
   root that is not counting is reported (`root-unchained`). The chain ends
   at the first generation without a counting root; its last canonical root
   is the current root, and every row before it (and every row of an earlier
   generation) is ignored for the current state (§2.11). Another counting
   root of any generation of the chain that differs from that generation's
   canonical root in device or epoch is an intrinsic contradiction
   (`conflicting-roots`). Only valid roots count toward it, as only valid
   rows take part in the same-transaction rule (step 3), so a forged or
   malformed row cannot raise it. Conflicting device-key rows, and a valid
   canonical root whose device has no keys in the view (possible only for
   an `app` root, which nothing signs), are intrinsic contradictions.
   Rejected roots never fail replay; the runtime reports them like
   unreadable rows (§4.3).

   **Conflicting generation-0 roots fail closed, permanently.** Two
   different valid roots of generation 0 can only come from the account's
   own sessions or the server: the server accepts at most one bootstrap
   (§0 item 5), so no legitimate flow produces them. They are a detectable
   integrity failure, not a state to resolve: the conflict fails the current
   state and every state as of a position after both roots, and a later
   reset does not escape it (the chain is checked at every generation). The
   account is unusable for E2EE; the remedy is a new account. The runtime
   surfaces `conflicting-roots` loudly, as a security alert like
   `root-not-mine`, never as an ordinary sync error (§7, §8).

2. **Approvals within an epoch.** Public approvals of the current epoch strictly
   after the epoch's position are applied in batches of equal position. An
   approval is accepted iff its signer was active _before the batch_, its
   recipient is not removed, its signature over the public approval bytes
   (§2.5) verifies, and it is not a non-recovery approval carrying a
   `recoverySignature`. A recovery approval (§2.9) is accepted regardless of
   the signer's membership. A false verification skips the row; a signer
   failure propagates. A missing recipient key is an intrinsic contradiction.
3. **Successors.** Visited one position at a time, in position order. A
   candidate is **valid** unless its account or predecessor differs, it is
   not strictly after the current epoch position, its epoch was already
   used, it is malformed, its public `revision` differs from the recomputed
   one, its signer was not active before it, its target is not an active
   device (removal) or not a valid, earlier, unretired recovery root
   (retirement), its signature fails, or its `membership` is not exactly the
   active set minus the target. Rows at one position were written in one
   transaction, which only the account itself or the server can produce;
   their order is undefined. So a valid candidate is accepted only if it is
   the only valid successor of the current epoch at its position and no
   valid recovery root (§2.9) is at that position; otherwise none of them
   is. Rows that fail any check, successors or recovery roots, never take
   part in this rule, so an invalid row cannot block a valid successor. The
   accepted successor advances the epoch.
4. Unsupported signing mechanisms or versions raise
   `UnsupportedE2EECapabilityError`; they are never treated as invalid. The
   same holds in private verification (below): a private record whose
   signer's request names an unsupported signing mechanism stops it with
   that error; it is not skipped.

The result is the current account epoch, its active devices, removed devices,
active and retired recovery roots.

**State as of a position.** Every check of the form "author (signer, sender)
device active, with account epoch `e`, at its position" uses the account's
state as of that position `p`: the replay above over the rows accepted before
`p` only, with the generation chosen among the roots accepted before `p`.
Every table is filtered, device-key rows included: a key projection accepted
at or after `p` does not exist for that state.
Only the current state uses the highest generation at the cut. So records an
account signed before a reset (§2.11) still verify against the generation
that was current when they were accepted. A removed device is never reactivated; it
needs a new device ID. Removal is per generation: a device removed before a
reset is not removed in the new generation, and can be approved there again;
only an active device of the new generation can approve it, so this grants
nothing the account's devices could not already grant.

**Root not mine.** A device of the account treats a canonical root that does
not match its own knowledge of the account as the loud contradiction
`root-not-mine`, surfaced to the user as a security alert, never as an
ordinary failure:

- a device holding the local-first identity: the canonical root is not a
  `local-first` root of its key;
- private verification (below): the account's own identity of the canonical
  generation names another device or epoch than the canonical root;
- a floor (below) whose root is not the canonical root of its generation in
  the chain.

For an app account the floor is the only check that catches a substitution
the server makes after the device first verified the account; before that
first observation the device trusts what it sees (§8).

**Floors.** After it verifies an account state, a device persists, in its
private store, the account's **floor**: the read's cut, the generation, the
canonical root of that generation, and the current epoch with its position.
A later read is refused with `stale-state` if its cut is earlier, its
generation lower, or (same generation) the floor's epoch is not in its
chain; it is `root-not-mine` if the chain's root of the floor's generation is
not the floor's root. A floor of the device's own account also records
whether the device was active. If it was, and the device is not active in
the newer state (it was removed, or the account moved to a generation it is
not part of), the outcome is `device-no-longer-active`. A device that was
offline while it was removed and the account was then reset sees exactly
this, as does an active device whose account another device reset, so it
is not a security alert: the device wipes its local E2EE state for that
account (held keys and floors, §6.4), enrols again with a new local device
(§2.1) and requests approval (§2.5); the SDK tells the user the device must
be approved again. `root-not-mine` stays reserved for a contradicted root:
the root of the floor's generation is not the one this device verified
(for example a different generation-0 root), whatever later generations
follow it. The checks run in that order: `stale-state`, then
`root-not-mine`, then `device-no-longer-active`. Otherwise the floor
advances to the newer state. The client runtime keeps floors for spaces as
well (§6.3a).

**Private verification.** A device that wants the account key also reads the
private tables (same cut) and accepts: the identity only if it matches the
public root (same generation, device, epoch and ledger version) at exactly
the same position; a private approval only if its
challenge and proof were accepted at or before it, the public approval with
the same ID (the challenge ID) was accepted by replay at the same position
with the same account, epoch, signer and device, request and key
projections match at its position, both the proof signature and the approval
signature verify; a private successor only if the accepted public successor
with the same ID is at the same position and has the same `accountId`,
`predecessor`, `epochId`, `signerId`, `action`, target column and
`membership`, its signer's request matches, every ID of its private
`revision` is a verified private approval of the predecessor epoch before it,
and its own signature verifies. The two `revision` fields list the same kind
of ID (challenge IDs) but are computed differently (authenticated private
approvals, and approvals public replay accepted, §2.8); each is checked, with
its signature, in its own domain. Private evidence is bound to the exact
current state it was verified against: it is never combined with a state as
of a position or with a state of another read.

### 2.5 Device approval by challenge and response

Let `P(column) = ctx{application: D, policy: "jazz.e2ee.device-approval.v1",
scope: "account", identifier: account, table: "__e2ee_device_challenges",
row: challengeId, column, epoch: challenge.epochId, recipient:
challenge.deviceId}`.

1. **Challenge** (stage `challenge`, approving device). The approver is active
   and holds the authenticated current account key; the requester is not
   removed and its request matches its keys. Generate a 32-byte secret `S`,
   kept only in memory. Insert challenge `{id, deviceId, epochId: current,
envelope: seal(requester.pub, P("challenge"), S)}`.
2. **Response** (stage `proof`, requesting device). The challenge targets this
   device in the current epoch. Open `S` (32 bytes). Insert proof `{id:
challengeId, proof: wrap(S, P("proof"), Z32), signature:
sign(P("proof-signature") ‖ frame[proof])}`.
3. **Completion** (stage `approval`, approving device). The challenge epoch
   equals both the challenge's and the current epoch. Possession is verified:
   `unwrap(S, P("proof"), proof)` yields `Z32` and the proof signature verifies
   under the request's signing key; anything else refuses ("invalid device
   possession proof"). With the current account key `K`, insert atomically:
   - approval `{id: challengeId, signerId, verification: wrap(K,
P("approval"), Z32), signature: sign(P("approval-signature:" ‖ signerId)
‖ frame[verification])}`;
   - public approval `{id: challengeId, accountId, epochId, deviceId, signerId,
signature}`
     signed over `ctx{application: D, policy:
"jazz.e2ee.public-device-approval.v1", scope: "account", identifier:
account, table: "__e2ee_public_device_approvals", row: approvalId, column:
"approval-signature:" ‖ signerId, epoch, recipient: deviceId}`;
   - delivery `{id: challengeId, envelope: E = seal(requester.pub,
P("delivery"), K), verification: wrap(K, P("delivery-verification") ‖
frame[E], Z32)}`.

   Every marker is unwrapped again and every signature verified before
   submission. The secret is erased when the operation ends.

Approval delivers the **account epoch key**, not any device private key.
Space keys reach the new device through its account: they are sealed to the
account's sealing keys, which the account epoch key derives (§2.12, §3.6).
Membership (the public approval) and key readiness (an openable delivery) are
separate: an approved device whose delivery is unusable is "waiting for keys",
not unapproved.

### 2.6 Acquiring the account key

`withAccountKey` tries, in order: the successor delivery to this device (or
the identity envelope for the root device at the root epoch), then every
verified approval delivery to this device for the current epoch. A candidate
that provably fails to open (malformed, or failed authentication) is skipped
and the next tried; a crypto provider failure fails the operation. A key is
accepted only after it authenticates the chain (§2.7). Recovery-key
acquisition (§2.9) and the possession check (§2.5) follow the same rule. (The
TypeScript implementation treated every `open` failure, provider failures
included, as an unusable candidate.)

### 2.7 Required and optional proofs

When authenticating account epoch key `K`:

- **Required:** the epoch's marker (identity `verification` or successor
  `verification`) and each successor `history` unwrap back to the root. An
  invalid marker or history (provably malformed or failed authentication, or a
  marker not `Z32`) makes the key unavailable; any other failure (provider
  error, cancellation, unsupported mechanism) propagates and stops the
  operation.
- **Optional:** each private approval's `verification` marker and, where
  deliveries are required, each delivery-verification marker. An optional
  proof is ignored **only** when provably invalid (an `InvalidKeyEnvelopeError`
  or a marker not `Z32`). Ignoring it does not revoke the device or change
  public membership; it only removes that approval from the authenticated set.
  Provider errors, cancellation, uncertain results and unsupported mechanisms
  stop the operation.
- **Signer lineage.** An approval counts only if its signer was authenticated
  before its batch, or it is recovery-authorised. A provably invalid approval
  therefore cannot found descendants' ancestry.
- **Revision completeness.** Walking history, the authenticated approval set
  of each predecessor epoch must equal the successor's private `revision`
  exactly; otherwise the key is unavailable.
- An approval-delivered key requires its delivering approval to be
  authenticated with its delivery marker.

### 2.8 Removing a device and rotating the account

Stage `account-rotation`; action `remove-device` (target: an active device) or
`retire-recovery-root` (target: an active recovery root). The author is active
and holds the current key `K`. A fresh epoch key `K'` and epoch ID are always
generated. Let `S(column, recipient) = ctx{application: D, policy:
"jazz.e2ee.account-successor.v1", scope: "account", identifier: account, table:
"__e2ee_account_successors", row: successorId, column, epoch: newEpoch,
recipient}`. Insert atomically:

- private successor: `membership` = ID list of surviving active devices;
  `revision` = ID list of authenticated private approvals of the predecessor
  epoch; `verification = wrap(K', S("verification"), Z32)`; `history =
wrap(K', S("history"), K)`; `deliveries` = delivery map of `seal(member.pub,
S("delivery", deviceId), K')` for exactly the members; signature over
  `S("signature", signerId) ‖ frame[predecessor, action, targetId,
retiredRecoveryRootIds, membership, revision, verification, history,
deliveries]`;
- public successor (same ID): same `membership`; `revision` = revision of the
  IDs of the public approval rows of the predecessor epoch that public replay
  accepted (§2.4, step 2) before the successor's position; signature
  over `ctx{application: D, policy: "jazz.e2ee.public-account-successor.v1",
scope: "account", identifier: account, table:
"__e2ee_public_account_successors", row: id, column: "signature", epoch:
newEpoch, recipient: signerId} ‖ frame[predecessor, action, targetId,
retiredRecoveryRootIds, membership, revision]`;
- a recovery delivery of `K'` for every unretired active recovery root whose
  sealing key is of the selected sealing mechanism; a root of another
  mechanism gets none (§0, mechanism agility) and does not block the
  rotation;
- the account sealing key record of the new epoch (§2.12).

`targetId` is the removed device or the retired root;
`retiredRecoveryRootIds` is the column's canonical bytes.

**Removing a device retires the recovery roots it held.** In an app
account, a device held a recovery root's key pair, and so the recovery
secret or material, when it registered that root (it derived or generated
the key pair to sign the root's keys) and when it became active by a
recovery approval with it (§2.9). Removing it therefore also retires, in the
same rotation, every root of the current generation that is still valid and
unretired before the removal and that the device either registered (its
`signerId`, accepted before the removal) or named in any of its accepted
recovery approvals (every one of them, not only the first: a device may
have recovered again after an earlier root was retired). The removal lists
exactly those roots in `retiredRecoveryRootIds`, the new key gets no
recovery delivery for them, and replay accepts a removal only if its list
is exactly that set (empty for a device that held none). The SDK then asks
the user to back up a new recovery secret, registered as in §2.9
("Retiring a derived root"). A local-first account's removals list none
(§2.9: its root derives from the account secret every device holds).
Otherwise removal does not retire recovery roots, and retiring a root does
not remove devices. Only one rotation from a given predecessor can win.

**What removal achieves depends on how the account signs in.** For an
account authenticated by the application's own sign-in, a removed device can
no longer obtain keys: it is not active, receives no delivery of the new
account key and cannot approve itself. Recovery keys and the recovery secret
are never persisted on a device (§2.9), and a device that used them had its
root retired by its removal (above), so whatever it kept does not open the
new key. For a **local-first account**, whoever holds
the account secret is the account: every device of the account stores the
secret in order to sign in, and the recovery root is derived from it (§2.9).
A removed local-first device therefore still signs in to Jazz as the account
(and keeps receiving the account's rows), derives the recovery key pair,
opens the recovery delivery of the new account key and approves a fresh
device ID with it. Removing a device does not protect against a device that
holds the account secret; the remedy for a lost or stolen local-first device
is to move to a new account. The SDK says so when removing a device of a
local-first account. Removal still serves its other purposes there (retiring
an unused install, rotating away a device whose secret was never stored, such
as a device that only held a link).

Both revisions therefore count only valid approvals. (The TypeScript
implementation counted every raw public approval row of the epoch, valid or
not, in the public revision.)

### 2.9 Recovery

Recovery builds on the account backup that local-first accounts already have.
Local-first accounts (`packages/jazz-tools/src/local-first-auth`) keep a
32-byte **account secret**, `jazz-auth-v1:` followed by 43 unpadded base64url
characters (`secret.ts`, `formatSecret`/`parseSecret`). It derives the
account's signing key and subject, so whoever holds it is the account
(README, "Threat model"). It is backed up either as a 24-word BIP39 recovery
phrase (`recovery-phrase.ts`) or in a platform passkey whose user handle is
the 32 secret bytes, created and read with user verification required
(`passkey.ts`, `BrowserPasskeyBackup`, exported as `jazz-tools/passkey-backup`).

- **Recovery secret.** 32 bytes `s` from which the account's recovery root is
  derived:
  - **Local-first accounts:** `s` is the account secret's 32 bytes. Restoring
    the account secret (by passkey or recovery phrase) restores E2EE access;
    there is no separate recovery material.
  - **Externally authenticated accounts** (the application's own sign-in):
    `s` is a random 32-byte secret in the same `jazz-auth-v1:` format,
    generated once per account. By default the SDK backs it up with the same
    passkey module, as a second credential whose display name says it is the
    encryption recovery key for the account; the recovery phrase form is also
    available. The SDK offers the backup when E2EE is first enabled on the
    account (creating a passkey needs a user gesture).
  - The passkey module is browser-only today (WebAuthn platform
    authenticators). Where it is unavailable, the recovery phrase of `s` is
    the default export, and exported material (below) remains.
- **Derivation.** Let `Dₐ = Dₐ(account)` and `g` the account's current
  generation (§2.11). At `g = 0`,
  `seed = BLAKE2b-256(key = s, input = UTF8("jazz.e2ee.recovery-seed.v1") ‖
0x00 ‖ UTF8(Dₐ))`; above it,
  `seed = BLAKE2b-256(key = s, input =
UTF8("jazz.e2ee.recovery-generation-seed.v1") ‖ 0x00 ‖ UTF8(Dₐ) ‖ 0x00 ‖
UTF8(decimal g))` (`Dₐ` is canonical JSON and contains no `0x00`). So the
  same secret gives a fresh, registrable root after each reset, while the
  roots of earlier generations stay invalid (§2.11). Then the sealing key pair is HPKE `DeriveKeyPair` of
  `BLAKE2b-256(key = seed, input = UTF8("jazz.e2ee.recovery-kem-key.v1") ‖
0x00)`, the signing key pair's Ed25519 seed is
  `BLAKE2b-256(key = seed, input = UTF8("jazz.e2ee.recovery-sign-key.v1") ‖
0x00)`, and the root ID is the first 16 bytes of
  `BLAKE2b-256(key = seed, input = UTF8("jazz.e2ee.recovery-root-id.v1") ‖
0x00)`
  with the version and variant bits set as for public derived IDs (§0). The labels
  are bound to the current mechanisms (`jazz.wrap.hpke-x25519` v1 and
  `jazz.sodium.sign` v1); a later mechanism defines its own labels and gives a
  second root, registered alongside. Every keyed derivation of this
  protocol and of the built-in mechanisms hashes `UTF8(label) ‖ 0x00 ‖ input`,
  the input possibly empty; the `0x00` separates the label from the
  domain, and including `Dₐ` gives different roots per application and
  environment from one secret. The derivation is pure: the same `s` and
  account always give the same root. Fixture: `s` = bytes `00`–`1f` and
  `Dₐ = ["0f8e3a4c-1d2b-4c5d-8e6f-7a8b9c0d1e2f","prod","alice"]` give root ID
  `a4c5c670-5336-81dc-a21b-a455befbf096`, sealing public key
  `4cb2da93696a09b991912b3c49e0a1fdd7278948be5c6d6cbff34239236bde78` and
  signing public key
  `16e6857ea0aa9e666c087026136fa793458b465150e5ba75213017edbd07466e`; as
  text, `s` is `jazz-auth-v1:AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8`.
  The same `s` and `Dₐ` give root ID `243d5113-0e7a-8965-b199-9c3e5db34384`
  at generation 1 and `0a84cdb6-4329-8e28-a6a5-e21185b04763` at
  generation 2.
- **Register.** After bootstrap (§2.3), and whenever the recovery secret is
  first available on an active device, the SDK registers the root derived
  for the account's current generation unless a valid root with that ID
  exists. Two publications. Stage
  `recovery-root`: an active device holding the account key inserts the root
  `{id: rootId, accountId, epochId: current, signerId, keys…}` signed over
  `frame[ctx{application: D, policy: "jazz.e2ee.recovery.v1", scope:
"account", identifier: account, table: "__e2ee_recovery_roots", row:
rootId, column: json(["root", signerId]), epoch}, env(signingMechanism,
signingPublicKey), env(mechanism, publicKey)]`. An existing root with the
  same ID must match exactly. Stage `recovery-delivery`: unless a usable
  delivery exists, insert `{rootId, epochId: current, envelope:
seal(root.pub, R, K)}` with `R = ctx{application: D, policy:
"jazz.e2ee.recovery.v1", scope: "account", identifier: account, table:
"__e2ee_recovery_deliveries", row: deliveryId, column: "envelope", epoch,
recipient: rootId}`. An unknown outcome of the first stage is not retried.
- **Exported recovery material (fallback).** For platforms without passkeys
  and for applications that want a root independent of any secret, the
  previous material remains. **Prepare** generates a random recipient keypair,
  signing keypair and root ID; nothing is published. The material is a secret
  string `json({format: "jazz-e2ee-recovery-v1", scope: Dₐ(account), rootId,
mechanism, version, publicKey, privateKey, signingMechanism, signingVersion,
signingPublicKey, signingPrivateKey})` with keys as byte arrays, handed to
  the user and kept outside the application. Generating material is not
  evidence that recovery is active. **Decode / inspect:** at most 2,000,000
  characters; exactly those eleven keys; matching scope and mechanisms; byte
  arrays 1–65,536. Both keypairs are checked by seal/open and sign/verify over
  `UTF8("jazz.e2ee.recovery-material-check.v1\0" ‖ scope ‖ "\0" ‖ rootId)`.
  Inspect is read-only and reports the current epoch, active devices,
  recovery roots and the validated root ID. Its root is registered exactly as
  above. (The recovery phrase of the recovery secret is the preferred export
  where passkeys are missing; material is for roots not derived from a
  secret.)
- **Validity.** A root is valid iff it is strictly after the account root, its
  `epochId` was the account epoch at its position, its signer was active then
  with a supported signing mechanism, and its signature verifies. In an app
  account, removing the registering device retires it (§2.8), since that
  device held its key pair; a local-first account's removals retire nothing.
- **Never persisted.** A device MUST NOT persist the recovery secret, the
  recovery seed, the recovery key pairs or exported material: they exist in
  memory for one registration or restore and are erased when it ends (a
  local-first account's secret is stored for sign-in by local-first auth, not
  by E2EE). Removing a device that registered a recovery root, or restored
  with one (with any of its recovery approvals), retires that root (§2.8).
- **Restore.** The device first obtains the recovery key pair: for a
  local-first account, by restoring the account secret (passkey or recovery
  phrase) and deriving; for an external account, by signing in with the
  application and restoring the recovery secret from its passkey (or phrase);
  or from exported material. The new device must have a request and not be
  removed. If it is
  already active with the same key, nothing is written. Otherwise it obtains
  the current key from a recovery delivery for the current epoch (each tried in
  turn), then runs §2.5 with itself as both approver and requester. The public
  approval uses policy `"jazz.e2ee.recovery-device-approval.v1"`, column
  `"recovery-signature:" ‖ rootId`, carries `recoveryRootId`, requires
  `signerId = deviceId`, and is signed by both the device and the recovery
  signing key over the same bytes. Replay accepts it if the root is valid,
  strictly earlier, and not retired at or before the approval. Private key
  authentication applies the same retirement check before letting a recovery
  approval found key lineage. (The TypeScript implementation checked
  retirement only in public replay.)
- **List active.** Valid, unretired roots, ordered by position then ID, each
  with `registeredByDeviceId = signerId`. No registration time is recorded.
- **Retire.** An account rotation with action `retire-recovery-root` (§2.8).
  Retirement is permanent: rows are immutable, a retired root receives no
  further deliveries and approvals using it at or after its retirement are
  ignored. A lost material file can still be retired by ID.
- **Retiring a derived root.** Because the derivation is fixed, a retired
  derived root can never be registered again from the same secret in the
  same generation.
  - **External accounts:** retiring is how a leaked or lost recovery secret is
    replaced. The SDK generates a new recovery secret, registers its root,
    offers a new passkey backup, and then retires the old root in one account
    rotation; the old passkey becomes useless for E2EE.
  - **Local-first accounts:** the account secret is the account, and it cannot
    be rotated without becoming a different account. Retiring its derived root
    therefore only turns off recovery by account secret for this account (the
    account may still register exported material); it does not protect against
    a leaked account secret, because whoever holds the secret signs in as the
    account. A leaked local-first account secret loses the account, E2EE
    included, and so does a lost or stolen device of a local-first account,
    which holds the secret (§2.8). Recovery stays derived from the account
    secret: there is no separate recovery secret and no device check on
    writes for local-first accounts. The SDK asks for confirmation before
    retiring a derived root of a local-first account.

### 2.10 Sign-out, account change and device linking

- **Sign-out** closes every live owner synchronously: in-flight operations are
  cancelled, owned secret buffers erased, pending approvals and recovery drafts
  disposed, and late results never reach a later session. Private records stay
  on disk (§6.4) so a later offline sign-in can reopen.
- **Account change.** An encryption handle bound to one account refuses
  operations for another ("does not match the admitted account").
- **Same-account upgrade.** A stored encryption handle follows an in-place
  upgrade of the same account and encryption configuration. It is invalidated
  by sign-out, by an account or configuration change, and by a reopen after a
  failed logout (the reopened session gets a fresh owner; the old handle stays
  closed). Owned approvals and drafts do not survive.
- **Linking a device to an account.** While the link is pending, operations
  wait for it within their own deadline; synchronous calls refuse. The handle
  follows only if the account and configuration are unchanged.

### 2.11 Account reset

An account whose devices are all lost, with no usable recovery root, cannot
otherwise use encryption again: no device can approve a new one, and
bootstrap (§2.3) refuses an account with history.

**Availability.** Reset exists only for accounts authenticated by the
application's own sign-in (for example WorkOS or Better Auth), and only when
the application opts in with `"allowAccountReset": true` in the top-level
`encryption` object of `jazz.app.json` (§1.5; off by default; carried in the
deploy request). A local-first account has no reset without its secret:
there the account secret is the account, and losing it is losing the
account. The SDK never offers a reset for one; a holder of the secret
restores access by recovery (§2.9) instead, which needs no new generation.
The server enforces both: R5 refuses an identity of generation above 0 from
a local-first session, for an account founded by a local-first identity
(also when it signs in through a linked identity of the application), or
when the deployed configuration does not allow reset. Clients enforce the
local-first part themselves: replay accepts a later generation of a
local-first account only if the account's local-first key signed its root
(§2.4 step 1), so nobody without the secret can reset it.

Stage `account-reset`. Preconditions, read at one cut: the account has a
canonical root of generation `g`; this device has a request whose keys match
it (as for bootstrap: `no-device-request`, `device-keys-mismatch`) and is not
an active device of generation `g` (`reset-device-active`: an active device
can approve new devices, so it never needs a reset). Then insert, exactly as a
bootstrap but with `generation: g + 1` and the reset identity ID (§1.2), an
identity, a public root and the account sealing key record. The contexts are those of §2.3 with `row` the
identity ID. At most one reset per generation is accepted (§0 item 5).

Effects, by replay (§2.4): the new root starts the account's history again.
Devices, approvals, successors and recovery roots of earlier generations are
inactive; a recovery root must be registered again: from the same recovery
secret, the root derived for the new generation (§2.9). Records the account
signed before the reset stay valid (state as of a position, §2.4).

**A reset account requests keys like any reader.** Its new generation holds
no account key of the old one, so it opens nothing sealed to its old account
(§2.12), and its runtime requests the space keys it needs (§3.4). There is no
approval: Jazz's read permission on each space row decides, as for every
reader (§8). Readers see the reset: a key request by an account whose
generation, as of the request's position, is above 0 is that space's **reset
notice**. Each reader's runtime verifies the generation from the account's
public roots (R10) and emits an `account-reset` encryption event `{account,
space, generation}` once per space, account and generation, next to the
unreadable-row reports (§4.3), so the application can show "<account>'s
encryption was reset". An account that reset before it ever read the space
is shown the same way.

### 2.12 Account sealing keys

Space keys are sealed to accounts, not devices (§3). For an account epoch
key `K` and a sealing mechanism `m`, the account's **sealing key pair** is
HPKE `DeriveKeyPair` of the input keying material
`BLAKE2b-256(key = K, input = UTF8("jazz.e2ee.account-kem-key.v1") ‖ 0x00 ‖
H(m))`, where `H(m)` is the common envelope header of `m` (its ID and
version, no payload); binding the mechanism means one epoch key never gives
two mechanisms the same input keying material. Every device holding `K`
derives it, and nothing private is stored. This is the existing key-sealing
mechanism (`seal`, §0) with the account's key as recipient.

The public half is published in `__e2ee_account_keys` `{accountId, epochId,
signerId, mechanism, version, publicKey, signature}`, signed by the device
`signerId` over `ctx{application: D, policy: "jazz.e2ee.account-key.v1",
scope: "account", identifier: account, table: "__e2ee_account_keys", row: id,
column: signerId, epoch: epochId} ‖ frame[env(mechanism, publicKey)]`. It is
written in the transaction that creates the epoch (bootstrap §2.3, rotation
§2.8, reset §2.11), and, for a selected mechanism with no record for the
current epoch (mechanism migration, §0), by any active device holding `K`.

A record is **valid** iff its epoch is in the account's chain, its signer
was active with that account epoch at its position (§2.4), and its signature
verifies. A sender seals to the first valid record (position, then ID) of
the account's current epoch and the selected mechanism; with none, sealing
fails with "recipient unsupported" and the sender waits. A device of the
account also checks that the record derives from `K`; one that does not is
the intrinsic contradiction `conflicting-account-key` (§7), since only the
account's own devices can write it.

Fixture: `K` = bytes `00`–`1f` and `m` = `jazz.wrap.hpke-x25519` v1
(`H(m)` = `4a45324501156a617a7a2e777261702e68706b652d78323535313900000001`)
give input keying material
`ac4df8236cf35730587626b161094aafe5fc81447b7b051142e49322fbdb0aea` and
public key `49dcff86a89b83bdde5eda452d647296878278a9fe29f2df2e93b1ad3b014773`.

**Account changes and space keys.** An envelope sealed to an account names
the account epoch whose sealing key it used: the one current when it was
sealed. Every device of the account holding a later epoch key of the same
generation reaches that epoch's key through the successors' `history` (§2.7)
and opens the envelope. A device removed by an account rotation (§2.8) keeps
the account keys it held, so it can still open what was sealed to those
epochs and nothing sealed afterwards. A reset starts a new chain, and the new
generation opens nothing sealed to the old one (§2.11).

## 3. Spaces

### 3.1 Records

Let `D = Dₐ(creatorAccountId)` of the space's root, `rootId` the root's row
ID, and `X(column, recipient, epoch) = ctx{application: D, policy:
"jazz.e2ee.space.v1", scope: scopeId, identifier, table: "__e2ee_spaces",
row: rootId, column, epoch, recipient}` (all four coordinates must be UUIDs).
`A(account, accountEpoch)` is that account's sealing public key for that
epoch (§2.12). `K` is the predecessor's key and `K'` the new one; in a
delivery, `K` is the key of `keyEpochId`.

| Record      | Signed bytes                                                                                                                                                            | Key material                                                                                                                                                                                                                                                                                                  |
| ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Root        | `frame[X(json(["root", creatorAccountId, deviceId, accountEpochId]), "", epochId), env(mechanism, verification), accountEnvelope]`                                      | `verification = wrap(K₀, X("verification", "", epochId), Z32)`; `accountEnvelope = seal(A(creatorAccountId, accountEpochId), X("account-envelope", json([creatorAccountId, accountEpochId]), epochId), K₀)`                                                                                                   |
| Successor   | `X(json(["successor", id, "signature"]), json([authorAccountId, authorDeviceId, authorEpochId]), epochId) ‖ frame[predecessor, verification, history, accountEnvelope]` | `verification = wrap(K', X("verification", "", epochId), Z32)`; `history = wrap(K', X(json(["successor", id, "history"]), "", epochId), K)`; `accountEnvelope = seal(A(authorAccountId, authorEpochId), X(json(["successor", id, "account-envelope"]), json([authorAccountId, authorEpochId]), epochId), K')` |
| Key request | Unsigned; its ID is derived (below)                                                                                                                                     | None                                                                                                                                                                                                                                                                                                          |
| Delivery    | Unsigned; its ID is its request's                                                                                                                                       | `envelope = seal(A(recipientAccountId, recipientAccountEpochId), X(json(["delivery", id, senderAccountId]), json([recipientAccountId, recipientAccountEpochId]), keyEpochId), K)`                                                                                                                             |

In the root, `env(mechanism, verification)` is applied literally: the root's
`mechanism` and `version` columns name the key wrapping mechanism of the
marker, and the signed field is the common envelope of that mechanism whose
payload is the whole `verification` column, which is itself already that
mechanism's envelope (`JE2E 01 ‖ len ‖ id ‖ u32be(version) ‖ verification`).

A key request's ID is the version 8 ID (§0) derived from
`json(["jazz.e2ee.key-request.v1", scopeId, identifier, accountId,
keyEpochId, attempt])`, `attempt` an unsigned 32-bit integer written in
decimal. Requests and deliveries are unsigned: the server authenticates their
author (`$createdBy`, R2), and a delivered key is accepted only after it
authenticates against its version's marker and the chain (§3.3), which the
signed root and successors carry.

### 3.2 Creation

A space exists once its root is accepted. Only an updater creates it (R13).
The creator generates the root epoch and `K₀`, and writes the root with
`accountEnvelope`, its own account's copy; no delivery is written.

- **With the space row.** When the SDK inserts a space row (a row of a table
  some `encryption` entry names as `scope`), it stages the root in the same
  transaction, so encrypted rows written right after need not wait for
  anything. It does so when Jazz's local permission evaluation says the
  session may update the row as the transaction leaves it. The authority's
  check (R13) decides, and its refusal refuses the transaction, row
  included; offline, see §5.4.
- **Rows without a space.** For a space row that has no root (it predates the
  encryption declaration, or its creator did not create the space), the first
  encrypted write by an updater creates the space in that write's
  transaction (§5.4), and so does a conversion sweep
  ([migrations](E2EE_MIGRATIONS.md) §3.5). Any updater may do so. A session
  that may not update the row gets `space-pending` for an encrypted write;
  a read of the space returns no encrypted rows (none can exist) and reports
  the space `space-pending` (§4.3).
- **Concurrent creators.** The authority accepts one root per address (R13);
  the others are refused, re-read, and request the accepted root's key.

Replay accepts a root iff its creator had an account root strictly before
it, its device was active with account epoch `accountEpochId` at its
position (§2.4), and its signature verifies. The space's root is the first
valid root of its address (position, then ID); the server accepts only one.

### 3.3 Key chain and acquiring a key

**Chain rule.** From the root's version, the next version is the first
successor of the address, in position order then ID, that names the current
version as `predecessor`, is strictly later than it, has an epoch not used
before, is well formed, whose author device was active with account epoch
`authorEpochId` at its position (§2.4), and whose signature verifies. The
last version reached is the **current version**. Whether the author could
update the space row is Jazz's decision and is not re-checked. Every reader
applies the same rule.

**Acquiring.** The client reads, at one cut, the root, the successors, the
deliveries addressed to its account (R12) and the public account histories
of the authors involved. The candidates for a version `v` are the
`accountEnvelope` of `v`'s root or successor when its own account wrote it,
then deliveries of `v` to its account. Each names an epoch of its account;
the device opens it with that epoch's sealing key, derived from the account
epoch key it reaches through its account's chain (§2.6, §2.7, §2.12). A
candidate naming an epoch it cannot reach (another generation) is skipped,
as is a delivery at or before the root's position. An opened key is accepted
only if `v`'s marker verifies and each `history` unwraps, back to the root, a
key whose marker verifies; every earlier version's key is thereby obtained
too. A candidate that provably fails to open or authenticate is skipped; a
crypto provider failure fails the operation (§0). The client tries the
current version first and then earlier ones, keeps every key it accepts as a
held key, and requests the current version when it does not obtain it
(§3.4).

**Unresolved history.** A record needing a capability this client lacks
(unsupported signing mechanism or ledger version) leaves the chain
unresolved from there (§7, unsupported capability); the client does not skip
it.

### 3.4 Key requests and deliveries

In the first release keys move **only** by request: nothing is delivered
that was not asked for, and the server never decides whether an account
other than the session's may read a row. Delivering proactively (to readers
at rotation, or to new readers when they gain access) would need that check
and is possible later work.

- **Requesting.** When a device needs a version it cannot acquire (§3.3)
  (a read or write needs it, or the chain shows a newer current version of
  a space it holds keys for), its runtime writes a key request
  `{scopeId, identifier, keyEpochId, accountId, attempt}` for the current
  version, automatically: applications and users never handle requests. The
  server accepts it only from a reader (R15). A refusal means the account
  may not read the space row: the device reports what it cannot decrypt
  `key-unavailable` and asks again only when a later read needs the key.
- **One open request.** A request is open until a delivery answers it.
  The account's devices write attempt `n` only when each lower attempt's ID
  is taken, either by a request of this account whose answer failed to open
  or authenticate, or by a row that is not a valid request of this account
  (an occupied ID, §0). So an account has at most one open request per space
  and version; two of its devices writing at once derive the same ID and one
  is accepted. Requests do not expire.
- **Answering.** Every device holding keys of a space watches its key
  requests (R12). For a request of a version it holds, a device checks the ID
  derivation (and ignores the request otherwise), replays the requester's
  public account history (R10), and finds the requester's current account
  epoch and its valid sealing key (§2.12); without one it waits. After a
  random delay (so that usually one reader answers) it inserts the delivery,
  with the request's ID, sealed to that key. The server accepts it only from
  a reader answering an accepted request (R16). At most one answer per
  request is accepted; a device whose answer is refused stops (a refusal
  looks the same whether another reader answered or policy refused).
- **Receiving.** The requester's devices watch deliveries addressed to
  their account and acquire the key (§3.3). An answer that does not open or
  authenticate leads to the next attempt. A reader that answers with bad keys
  is named by `senderAccountId`; it is a reader, so this costs it nothing it
  could not already do (it could disclose the key).
- **After losing access.** A request accepted while its author could read the
  space row may still be answered after that access ends; the answer carries
  only the version requested, which was current while the author could read.

### 3.5 Rotation and removal

**Rotation** (`db.encryption.rotate(space)`) is by an updater (R14) whose
device is active and holds the current version (it acquires or requests it
first, §3.3–§3.4). It generates a new epoch and `K'`, and inserts one
successor in a ledger transaction that read the chain at its cut;
read-set validation refuses a second successor of the same predecessor, and
the loser re-reads. Nothing is delivered at rotation: the rotator's account
holds `K'` through the successor's `accountEnvelope`, and every other reader
requests `K'` when it needs it (§3.4). Right after a rotation the rotator's
account is the only holder of the new version (§8).

Rotation does not re-encrypt existing content: old cells stay under their
versions, which the newest key opens. Every rotation adds a version; reads
use the held key of each cell's version and equality queries compute one
token per held version (§4.5). Until a reader obtains the new version, it
writes with the newest one it holds (§5.2).

**Removal.** E2EE does not detect removals. To exclude someone, the
application removes their read access to the space row and then calls
`rotate(space)`; developer documentation must say so, and that the order
matters (a rotation accepted before the access change excludes nobody).
The removed account's request for the new version is refused (R15), and Jazz
stops sending it the space's rows. It keeps every key it already held, which
stay usable for content of their versions (§8).

### 3.6 New devices, recovery and account changes

Space keys are held per device and sealed per account. A device approved
(§2.5) or restored by recovery (§2.9) obtains its account's key and then
acquires every space key sealed to its account, from the envelopes of roots
and successors its account wrote and from deliveries to its account (§3.3);
no other account and no other device is involved. An account rotation changes
the sealing key later envelopes use; the account's remaining devices still
open earlier ones, and a removed device opens only what it could before
(§2.12). A reset account requests again (§2.11).

### 3.7 Offline

Provisional operations replay over a provisional snapshot (§0), keeping the
author's account-history predicate (active device, not removed, keys and
mechanisms matching). Space creation may be offline (§5.4). A key request
written offline is submitted on reconnection; answering and rotation are
online only. On reconnection, Jazz permissions decide acceptance.

## 4. Reads

### 4.1 Stored cells

An encrypted cell is stored as
`env(("jazz.e2ee.cell-record", 1), epochId ‖ cipherOutput)`, where `epochId`
is the 36 ASCII bytes of the space epoch UUID and `cipherOutput` is the cell
cipher's own envelope. The epoch is in clear and also authenticated. The cell
cipher's context is `frame[ctx{application: Dₛ, policy:
"jazz.e2ee.cell-record.v1", scope: scopeId, identifier, table: tableId, row:
rowId, column: columnId, epoch: epochId}, cellType]`, where `tableId` and
`columnId` are Jazz's stable catalogue identities and `cellType` is the
canonical JSON of the column's **sealed type**: the logical column type
(including nullability, enum variants and JSON schema) it was introduced with.
When a later schema widens the type in an encoding-compatible way, the
declaration pins the original as `sealedType` and `cellType` stays that
([migrations](E2EE_MIGRATIONS.md) §4.3); otherwise the sealed type is the
declared type. Changing any of these otherwise makes old cells unreadable.
Plaintext is Jazz's native encoding of the single value.

**Short values.** A stored encrypted value is never ciphertext when it is
shorter than a cell record (47 payload bytes). Two such values have fixed
meanings and are never reported as malformed:

- The **empty byte string** means "never written": the row predates the
  column, which a lens added with the empty default. The reader returns the
  column's declared plaintext default, or null for an optional column. When
  the empty value is stored in the row (rather than supplied by the lens for
  a row that predates the column), any writer Jazz permits could have put it
  there, so the row is returned with the default and the cell is reported
  `empty-cell`; the read does not fail. Core tells the two apart (H3).
- The **cleared marker** `4a 45 32 58 01` means "moved by a schema
  conversion". A reader whose declaration names this column as the source or
  target of a conversion treats it as absent and applies the conversion rule
  ([migrations](E2EE_MIGRATIONS.md) §3.2); any other reader leaves the row out
  and reports the cell `superseded`.

Any other value shorter than a cell record is `invalid-ciphertext`.

A large value is stored as a stream record: an
`env(("jazz.e2ee.stream-record", 1), …)` routing record naming the epoch,
followed by the stream ciphertext, with the stream mechanism's header carried
once; the exact layout is in the large-value format. Its context uses policy
`"jazz.e2ee.stream-record.v1"`; only non-indexed `bytes` columns may stream.
An equality token is stored raw in `__e2ee_eq_<column>` (no epoch prefix),
computed with policy `"jazz.e2ee.equality.v1"`, no row, and `-0` normalised.

The byte layouts of both wrappers are pinned in the format specs:
[cell record](E2EE_CRYPTO_FORMAT.md#cell-record-version-1) and
[stream record](E2EE_LARGE_VALUE_FORMAT.md#stream-record-version-1). The
large-value format carries the stream mechanism's header once.

### 4.2 Key selection

- A read decrypts with **any** key this device holds for the space, online or
  offline. Reads perform **no** access check of their own.
- Each cell names its version; the reader uses the held key of that version.
- A key is acquired (§3.3) only when the device holds no key for the space at
  all, or holds keys but a row needs a version it does not hold; at most once
  per read. When an acquisition shows that an epoch is not in the space's
  chain, the device remembers that until the chain it knows advances, and
  reports cells of that epoch `invalid-ciphertext` without acquiring again;
  otherwise a live query pays the acquisition again on each refresh.
- When the acquisition obtains no key for a needed version, the device's
  request for the current version is open (§3.4): the read returns the rows
  its held keys decrypt, leaves the space's other rows out and reports the
  space once as `key-pending` (§4.3). It does not fail and reports no row
  `key-unavailable`. When the delivery arrives, the device acquires the key,
  the report is withdrawn and live queries receive the rows as additions.
- When the request is refused (the account may not read the space row), the
  read returns what its held keys decrypt and reports the rest
  `key-unavailable`. Any other acquisition failure (provider error,
  cancellation) fails the read.
- A held-key record that fails its checks (§6.2) is skipped and reported; the
  read continues with the other held keys and acquires that version again if
  it can. (The TypeScript implementation failed every read of the space.)
- Keys obtained by an acquisition, including earlier versions from the
  history chain, become held keys.
- A device watches deliveries addressed to its account and the chain of each
  held space; when a delivery or a new version syncs in, it acquires (and if
  needed requests) without waiting for a read. A failed acquisition is
  retried with backoff until the key is held, a later version supersedes it,
  or the session closes.

### 4.3 Unreadable rows

An ordinary query or subscription leaves out a row with an undecryptable
encrypted cell and returns the rest; no partial plaintext is returned. Report
`{table, id, column, code, space}`:

| Code                 | Meaning                                                                                                                                 |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `invalid-ciphertext` | Authentication failed; the cell belongs to another row, table, column or space; malformed; no such epoch in the space's chain           |
| `unsupported-format` | Unknown envelope mechanism or version                                                                                                   |
| `key-unavailable`    | This device lacks that version's key and cannot obtain it (its request was refused, §3.4)                                               |
| `key-pending`        | Per space, not per row: this account's key request is open (§3.4)                                                                       |
| `space-pending`      | Per space, like `key-pending`: the space row has no space yet (§3.2)                                                                    |
| `empty-cell`         | The stored cell is the empty byte string; the row is returned with the column's default (§4.1)                                          |
| `superseded`         | The cell holds the cleared marker (§4.1) and this reader's declaration names no conversion for the column                               |
| `stale-space`        | The cell was sealed under another space of the same row's history; it was read and needs repair ([migrations](E2EE_MIGRATIONS.md) §3.9) |

A `key-pending` or `space-pending` report carries `{table, space, code}` with
no `id` or `column`, and is deduplicated by `(table, space)`; it is not repeated for
each row. It is withdrawn when the key is obtained, and live queries then
receive the space's rows as additions. A `stale-space` row is returned (it
decrypted) and reported. A row whose space coordinate is not a string is
reported against the space column as `invalid-ciphertext`. Any other error fails the read. Reports are
deduplicated per account by `(table, id, column, code)` in a bounded
least-recently-used set of 10,000 entries; a listener registered later first
receives the reports still in that set. A console warning names table, row and
column, never content. (The TypeScript implementation cleared the whole set on
reaching the bound.)

Unreadable rows are dropped after `limit` and `offset` have been applied, so a
page may contain fewer rows than its limit. This is accepted behaviour: the
dropped rows are reported, and a client must not treat a short page as the end
of the results.

### 4.4 Strict reads

Reads inside a transaction, mutation pre-image reads and authority reads are
strict: any bad cell fails the read, so a write never rests on skipped rows.

### 4.5 Equality-filtered queries

Encrypted predicates must be `eq` (or a complete enum `match`) on a column
declared with an equality index, together with a plaintext `eq` on the space
column. Tags are per key version: a query computes one token per held
version, and the physical predicate becomes `__e2ee_eq_<column> IN tokens`
(the server can link those tokens to one another, §8). Limit and offset are
applied after each candidate is decrypted and its authenticated value
compared. If the version set changes during the query it is retried (shared
budget of 3 attempts). Offline, no tokens are computed: every candidate of
the space is decrypted and compared. Ordering by encrypted columns,
traversals, unions and partial selects are refused as unsupported; they
never fall back to a scan. Includes of encrypted child tables are refused
too (a project list with its latest task takes two queries); developer
documentation should say so.

**Cold queries.** Online, a query with an encrypted predicate on a space for
which this device holds no key cannot compute any token, and an empty
`IN ()` would return a false empty result without ever revealing a missing
version. Planning therefore reports the missing space key, and the runtime
acquires it (§3.3) within the read's own budget before executing;
acquisition outcomes are those of §4.2 (an open request gives `key-pending`
and no rows). The query then runs against the version set it was planned
with.

**Completeness.** An equality query returns matching rows of the versions
this device holds. When the device knows (from the chain it last read) that
the space has versions it does not hold, the result may miss rows of those
versions, and the query reports the space once as `key-pending` (request
open) or `key-unavailable` (refused) so the result is not mistaken for
complete.

### 4.6 Subscriptions

A subscription whose key request is open waits without emitting, in one
bounded episode (the operation timeout, 10 s by default). Delivery or
authority changes trigger fresh reads; there is no polling and retries do not
extend the deadline. On expiry it emits the rows its held keys decrypt,
reports the space `key-pending`, and keeps watching; the rows arrive as
additions once the key is obtained. The reader that can answer may be offline
for much longer than one episode, so the wait is reported, not treated as an
error. One-shot reads do not wait.

While it waits, the subscription delivers nothing, but core keeps computing
deltas against its own baseline. The subscription wrapper therefore keeps a
full copy of the latest result (applying each withheld delta to it) and, when
the wait ends, emits that whole result as one reset (a snapshot replacing
whatever the subscriber had), after which it forwards deltas unchanged.
Boundaries: on key arrival or expiry it emits the reset; on stop, sign-out or
account change it discards the copy and emits nothing; a held-key removal
during the wait changes nothing until core's next refresh, whose delta the
copy absorbs.

## 5. Writes

### 5.1 Held key permits the write

A held key permits encrypting and submitting an ordinary Jazz write. The
content-write path adds no check of access, device approval, version
freshness or Global acceptance; only Jazz permissions accept or reject
the write. Key binding, session lifetime and cancellation still apply. When no
key is held, key acquisition (§3.3) happens before the write proceeds, and a
write that needs a key whose request is open waits for it within its own
deadline.

A device that has not yet obtained the newest version writes with the newest
one it holds (§5.2).

Deletes, and updates or restores that touch no encrypted column, need no key. Partial edits of encrypted columns are refused.
An update re-encrypts only the supplied encrypted columns, so one row may
carry cells of different versions. An encrypted row cannot change its space
coordinate through an ordinary write. The one exception is the `move` write
of [migrations](E2EE_MIGRATIONS.md) §4.4: a serializable, online-only write
that re-encrypts every encrypted column under the target space and changes
the space column in one transaction, with held keys of both spaces.

### 5.2 Which version

A write uses the newest authenticated key available to this device: the held
key with the greatest chain depth. A held key's `position` always means its
version's depth in the chain (root = 0); one chain orders by depth alone, so
the ledger position is not needed. When acquiring, the device starts from
the current version and, if it cannot open that version's key (its delivery
has not arrived), steps back through predecessors; a version not yet
delivered therefore does not block writes. Equality tokens use the same key
and version.

### 5.3 Offline and provisional writes

A write is provisional exactly when reads would use retained local state.
With a held key, an offline write records no access predicate. Offline space
creation keeps the author's device-active predicate (§3.7).

### 5.4 Space creation, including offline

An encrypted write to a space with no root, by an updater, creates the space
(§3.2) in the same transaction; any other session gets `space-pending`.
Inserting a space row creates the space with it (§3.2). The new key is stored
as a held key before commit, with a persisted `unpublished` mark (§6.2)
carrying the creation's durable transaction identity, and the mark is
cleared once the creation is accepted. An unpublished key never wins a tie
against a published key at the same depth, including after a restart.

Writes under an unpublished key depend on the creation: the runtime submits
each after the creation's transaction and makes it fail if the creation
fails. The key is discarded only when the creation is definitively not
accepted (refused at Local or Global, or cancelled before possible
submission): the epoch is fenced in memory, leases erased, only the record
matching this device and exactly that root, epoch and signature is removed,
and the writes made under it are refused with the creation's error. An
unknown outcome keeps it. After a restart the runtime reconciles each
`unpublished` record by its stored transaction identity
(`transaction_fate`): accepted clears the mark, refused discards the record
(and refuses its dependent writes), pending and unknown keep it; if the
identity is unknown to the store and an authenticated root of the address is
readable, the mark is cleared when it is this record's root, and the record
is discarded when another root won. (The TypeScript implementation kept the
mark in memory only.)

### 5.5 Streaming large values

Sources are distinct one-shot inputs; only non-indexed `bytes` columns
stream. Metadata is validated and encrypted before any source is pulled. The
operation timer pauses while a source is consumed and restarts with a full
budget for publication. Before attaching, the publication checks that its
dependencies (or, for mergeable content, the space coordinate) are unchanged.
An interrupted source is never resumed; a new attempt starts from a fresh
source.

### 5.6 Atomicity

- One public mutation or transaction callback is one Jazz transaction;
  callbacks are never replayed.
- Space creation commits with its space row or its first encrypted rows.
- **Keys inside a callback transaction.** A staging call that needs a key
  this device does not hold returns "needs key". In a binding whose
  transaction callback is asynchronous, the staging call awaits acquisition
  (§3.3) and retries; the callback itself is not replayed. The synchronous
  `Db::transaction(cb)` cannot await, so such a staging call fails with
  `key-unavailable` and the transaction aborts; the application acquires keys
  first with `db.encryption.acquire(space)` and runs the transaction again.
  (The TypeScript implementation had "content-key declarations"; they are
  dropped.)
- Unknown outcomes are reconciled by the original transaction's identity, not
  by resubmitting.

## 6. Private local state

### 6.1 Store

Private state is kept in a store separate from authentication storage. Per
scope `Dₐ(account)` it holds one device document and one held-key document
per space the device holds keys for, so storing a key rewrites
only that space's document:

```text
device document:
{ "format": "jazz-e2ee-local-devices-v2",
  "devices": [ { scope, id, mechanism, version, publicKey, privateKey,
                 signingMechanism, signingVersion,
                 signingPublicKey, signingPrivateKey } … ] }

held-key document (one per space root):
{ "format": "jazz-e2ee-held-space-v1", "revision": n,
  "lineage": <lineage record>, "keys": [ held-key record … ] }
```

`devices` holds at most one device, the one of the document's scope. An
unknown format is refused and never overwritten.

Device private keys are never stored in the clear:

- **Browser:** each private key is wrapped with a non-extractable WebCrypto
  key kept in IndexedDB; scripts can use the wrapping key through WebCrypto but
  cannot export it.
- **Apple platforms:** Keychain. **Android:** Android Keystore.
- **Other platforms:** app-supplied secure storage.

(The TypeScript implementation stored device private keys as plain JSON byte
arrays in an application-supplied store; the device record also carried an
unused 32-byte `challenge`, now dropped.)

### 6.2 Held-key records

Each held-key document stores the space's root and lineage once, and one
small record per held version that refers to it by depth:

```text
lineage record:
{ format: "jazz-e2ee-held-lineage-v1",
  binding: { application: Dₐ(self), device: deviceId, keys: <device hash> },
  scopeTable, root: <space root row, bytes as arrays>,
  successors: [ <space successor row>, … ],
  signature: [..] }

held-key record (one per epoch):
{ format: "jazz-e2ee-held-key-v3",
  binding: { application: Dₐ(self), device: deviceId, keys: <device hash> },
  rootId, epochId, depth: "<decimal>",
  prefix: sha256hex(canonical([root, successors[0 .. depth]])),
  unpublished?: { transaction: "<durable transaction identity>" },
  envelope: [..], signature: [..] }
```

`<device hash>` is `sha256hex(canonical([publicKey, signingPublicKey,
envelopeMechanism, signingMechanism]))`. `root` is the original space root
(its own row ID, address and creator domain); `successors` are the accepted
successor rows from the root's epoch onward in chain order, as far as the
deepest held epoch, appended as deeper keys are kept. A held-key record names
its exact `epochId`, its `depth` (§5.2) and the hash of the lineage prefix up
to it. `canonical` sorts object keys by code unit, drops undefined, and
encodes bytes as arrays and big integers as decimal strings; `B(f, x) =
UTF8(json(canonical([f, x])))` with `f` the record's format. The lineage
record is signed by the device over `B(format, record without signature)`.
A key is sealed to the device: `envelope = seal(device.pub, B(format, body),
K)` with `body` the record without envelope and signature; then `signature =
sign(device.signingKey, B(format, body + envelope))`. Storage grows linearly
with the number of epochs, and the store may split documents further.

On load, the lineage record is accepted only if its binding matches the
current device, its signature verifies, the root decodes and is a root of
its address, and `successors` is a chain (each successor's
`predecessor` is the previous epoch, starting at the root's epoch, epochs
unused). A held-key record is accepted only if: its binding matches; its
signature verifies; `rootId` is the lineage's root; `depth` is at most the
lineage length, `prefix` equals the hash of the lineage prefix of that depth
and `epochId` is the epoch at that depth (the root's at depth 0); the
envelope opens; and the marker of `epochId` is valid for the opened key: the
root's `verification` at depth 0, otherwise the `verification` of the
successor at that depth (both `X("verification", "", epochId)` under the
creator's domain, §3.1). The signatures and author activity of the lineage
were checked when the key was acquired (§3.3); the device's own
signatures protect them since. A record that fails is skipped and reported,
never fatal to the read (§4.2); a failed lineage record skips the space's
keys and the space is acquired again. Nothing in either record depends on the
application schema, so a schema update never invalidates held keys; the cell
context binds table, column and type (§4.1). Keeping a key replaces any
record of the same device, root and epoch. `unpublished` is present only for
a space creation not yet accepted (§5.4), carries the creation's durable
transaction identity for reconciliation after a restart, and is removed by
rewriting the record. The lineage records also give the runtime its
epoch-to-space lookup ([migrations](E2EE_MIGRATIONS.md) §3.9).

(The TypeScript implementation's `jazz-e2ee-held-key-v1` record bound a
`schema` hash, which it signed but did not check, stored a position instead
of the epoch and its lineage, had no persisted `unpublished` field, and
failed the read on an invalid record.)

### 6.3 Binding

All private records are bound to the application id, the environment and the
account (`Dₐ(account)`), not to the server URL. One install pointed at two
datasets with the same application id and environment shares device keys and
held keys.

### 6.3a Floors

The runtime persists, next to the held-key documents of each scope `Dₐ(self)`,
a **floor** for every account and space whose state it has verified:

- an account: the cut, generation, canonical root of that generation, and
  current epoch with its position (§2.4, "Floors"); for the device's own
  account and for every account it replays (authors, requesters);
- a space: the cut, the root ID, and the current version with its chain
  depth.

Each floor is signed by the device like a lineage record (`B(format, …)`,
format `jazz-e2ee-floor-v1`) and advances only forward, after a verification
at a newer cut. Every later read is checked against the floor before its
state is used: older evidence (an earlier cut, a lower generation, a chain
that no longer reaches the floor's epoch) is refused with `stale-state`, and
the runtime reads again; it never decrypts, writes or delivers with the older
state. An account whose chain no longer contains the floor's root is
`root-not-mine`, and an own-account floor whose device is no longer active is
`device-no-longer-active` (§2.4). Floors are wiped with the held keys (§6.4).
Protection starts at the device's first observation of each account or
space: before it, the device has nothing to compare with (§8).

### 6.4 Wipe rules

- Normal sign-out retains device and held-key records, so a later offline
  sign-in can read.
- With `clearOnSignOut`, the **last tab out** removes the account's device
  record and every held key bound to it (for a local-first store, together with
  the store's deletion). A tab that signs out while another holds the store
  keeps them. The wipe deletes the client's core storage first and the
  private E2EE records second; if the second step fails, sign-out reports an
  unfinished wipe.
- `deleteClientStorage` removes the namespace's storage and the registered
  private records, and invalidates live handles. It revokes nothing remotely.
- Access changes never remove held keys. Records are removed only by
  sign-out wipe, account change wipe, device removal, re-enrolment after
  `device-no-longer-active` (§2.4, "Floors": the account's device record,
  held keys and floors), explicit deletion, and discard of a failed space
  creation (§5.4). A wiped store is never recreated by a late operation.
- A held-key revision counter advances on every removal, so in-flight reads
  notice and retry (3 attempts).

## 7. Failure taxonomy

Every operation keeps its original error. Publication errors carry the
original record IDs and, when the refusal has a specific cause, that cause's
code (for example `recovery-root-mismatch`); a specific failure is never
flattened into a bare `refused`. The data-error codes are exactly
`key-unavailable`, `key-pending`, `space-pending`, `unsupported-format`,
`invalid-ciphertext`, `empty-cell`, `superseded`, `stale-space` and
`encryption-failed`. The encryption status event (not a data error) is
`account-reset` (§2.11). Account roots that do not count are reported with
`root-malformed`, `root-foreign-account`, `root-key-mismatch`,
`root-device-keys-missing`, `root-signature-invalid`, `root-kind-mismatch`
or `root-unchained` (§2.4 step 1); they never fail replay.
`conflicting-roots` of generation 0 (§2.4 step 1) makes the account
permanently unusable for E2EE (the remedy is a new account) and is surfaced
as a security alert, like `root-not-mine`. `root-not-mine` (§2.4) is a security alert
the SDK surfaces to the user: the account's root is not the one its own
devices know. `device-no-longer-active` (§2.4, "Floors") is not an alert:
this device was removed or the account was reset without it; the device
wipes its E2EE state for the account and enrols again. `stale-state` (§2.4, "Floors") refuses evidence older than
state the device already verified.
Every reader from the first release knows all of them, so that rows written
by a later schema version degrade to a reported gap. An equality query on a
space whose conversion or tag backfill is unfinished fails with the query
error `conversion-pending` ([migrations](E2EE_MIGRATIONS.md) §3.3), which
names the table, column, space and pending count.

| Outcome                     | TypeScript form                                                                                                                                                                                  | Must                                                                                                | Must not                                                                                       |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| Device approval pending     | `AccountKeyUnavailable` / readiness `not-verified`                                                                                                                                               | Show "waiting for approval or keys"; complete approval or recovery                                  | Re-approve, or treat the account as signed out                                                 |
| Key pending                 | `SpaceKeyDeliveryPending`                                                                                                                                                                        | Report the space `key-pending`; subscriptions wait one bounded episode, then emit and keep watching | Poll, fail the read, or report each row `key-unavailable`                                      |
| Key unavailable             | `E2eeDataError("key-unavailable")`, `SpaceKeyUnavailable`, `SpaceKeyNotMember`                                                                                                                   | Return what held keys decrypt; report the rest                                                      | Write plaintext, or request again within the read                                              |
| Space pending               | None (new)                                                                                                                                                                                       | Refuse the write `space-pending`; report the read; watch for the root                               | Create the space without the right, or write plaintext                                         |
| Superseded / stale space    | None (new, schema conversions)                                                                                                                                                                   | Report; leave a `superseded` row out; return a `stale-space` row                                    | Report either as `invalid-ciphertext`                                                          |
| Permission refusal          | Jazz rejection; `*PublicationError` outcome `refused`                                                                                                                                            | Report; reflect in local state                                                                      | Treat key possession as overriding it                                                          |
| Authentication failure      | `E2eeDataError("invalid-ciphertext")`, `InvalidKeyEnvelopeError`                                                                                                                                 | Leave the row out (ordinary reads) or fail (strict reads)                                           | Return unverified plaintext                                                                    |
| Unsupported format          | `E2eeDataError("unsupported-format")`                                                                                                                                                            | Report                                                                                              | Treat as invalid ciphertext                                                                    |
| Encryption failed           | `E2eeDataError("encryption-failed")`                                                                                                                                                             | Fail the write                                                                                      | Submit plaintext                                                                               |
| Provider failure            | The mechanism's `CryptoFailure` (distinct from `InvalidCiphertext`, `InvalidInput`, `NoKeyForMechanism` and a `false` verification), and a writer's failure to read back output it just produced | Fail the operation loudly; preserve the error                                                       | Report it as key unavailable, corrupted ciphertext or an invalid proof; try the next candidate |
| Unsupported capability      | `UnsupportedE2EECapabilityError` (`ledger-version`, `signing-mechanism`, `signing-version`)                                                                                                      | Leave the dependent state unresolved                                                                | Treat the record as invalid                                                                    |
| Intrinsic contradiction     | `IntrinsicAccountHistoryError` (`conflicting-roots`, `conflicting-device-keys`, `conflicting-account-key`, `missing-root-key`, `missing-recipient-key`, `missing-signer-key`)                    | Fail current replay; skip only a candidate decided at a fixed signed cutoff                         | Pick one of the conflicting rows                                                               |
| Unknown publication outcome | `AccountPublicationError` / `SpacePublicationError` outcome `unknown`                                                                                                                            | Reconcile the original operation by its record IDs                                                  | Resubmit blindly or replay a callback or source                                                |
| Recovery failures           | `E2eeRecoveryError`: `recovery-cancelled`, `recovery-material-unusable`, `recovery-root-mismatch`, `recovery-delivery-missing`, `recovery-delivery-unusable`                                     | Only candidate failures permit trying another delivery                                              | Attach private material to the error                                                           |
| Unsupported shape           | Typed "Unsupported encrypted query…" errors                                                                                                                                                      | Name the operation                                                                                  | Fall back to a scan                                                                            |
| Timeout / cancellation      | `TimeoutError`, `AbortError`                                                                                                                                                                     | Erase owned secrets; stop unsubmitted work                                                          | Claim a submitted operation failed                                                             |

Publication stages: account `request`, `bootstrap`, `challenge`, `proof`,
`approval`, `account-rotation`, `account-key`, `recovery-root`,
`recovery-delivery`, `account-reset`; space `create`, `rotate`,
`key-request`, `delivery`, `write`.

Serialization and transaction conflicts are retried internally only by
operations that are safe to repeat because they re-read their evidence and
stage fresh rows: the runtime's own publications (space creation outside an
application transaction, rotation, key requests and answers), conversion
sweeps, and a staging call retried after "needs key" before submission
(§5.6). A public mutation or transaction callback is never retried or
replayed: its conflict is returned to the caller, and an unknown outcome is
reconciled by its transaction identity.

(The TypeScript implementation wrapped every failure inside a publication as
a bare `refused` with the stage name, and defined two data-error codes,
`key-not-shared` and `maintenance-required`, that nothing raised; both are
dropped.)

Refusals that had no code in the TypeScript implementation (only a message)
get these codes:

| Code                            | Refusal                                                                                                          |
| ------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `account-has-history`           | Bootstrap found existing account evidence (§2.3).                                                                |
| `device-identity-conflict`      | A device request or key row exists with different keys (§2.2).                                                   |
| `device-keys-mismatch`          | The device's keys do not match its accepted request.                                                             |
| `no-device-request`             | Bootstrap or reset found no request (or more than one) for this device (§2.3, §2.11).                            |
| `reset-device-active`           | An account reset by a device that is still active in the current generation (§2.11).                             |
| `device-not-active`             | The acting device is not an active device of the account.                                                        |
| `device-removed`                | The device was removed and needs a new device ID.                                                                |
| `stale-challenge`               | The challenge is unknown, for another device, or not of the current account epoch (§2.5).                        |
| `approval-closed`               | The approval handle was closed or belongs to another operation.                                                  |
| `invalid-possession-proof`      | The requesting device's possession proof failed (§2.5).                                                          |
| `target-not-active`             | The device or recovery root to remove or retire is not active (§2.8).                                            |
| `unsupported-recipient`         | A recipient device, recovery root or account has no key of the selected mechanism (§0, §2.12).                   |
| `account-reset-not-allowed`     | An account reset by a local-first session, or where the application has not enabled `allowAccountReset` (§2.11). |
| `space-pending`                 | An encrypted write to a space that has no root, by a session that may not update its space row (§3.2).           |
| `rotation-needs-key`            | A rotation by a device that holds no authenticated key of the current version (§3.5).                            |
| `local-first-identity-mismatch` | A local-first root written with an identity whose key does not derive the account ID (§2.3).                     |

## 8. Security properties and limits

**Trust boundary.** Jazz permissions alone decide who may read a space: the
space row's read rule. E2EE protects content against ciphertext leaking
outside Jazz: a leaked database or backup, a copied replica, or an operator
who reads storage. It does not protect against a server, an operator or a
permission bug that grants read access to someone (that session can request
and receive the keys, §3.4), nor against a compromised login (whoever signs
in as an account with an active device, or resets an app account, is that
account).

Properties:

- Only devices that obtained a version's key can read cells of that version.
  Cells, tokens and streams are bound to application, environment, space,
  table, row, column, version and logical type; moving ciphertext between any
  of these fails authentication.
- Account records, space roots and successors are signed by an active device
  of their author and replayed in accepted order; forged or reordered records
  are skipped, contradictions fail closed. A delivered key is used only after
  it authenticates against its version's marker and the signed chain (§3.3).
- Unsupported or unverifiable evidence is never treated as invalid, and
  provider errors are never treated as proof of invalidity.
- Error messages carry fixed text and public coordinates, never plaintext,
  keys or provider diagnostics.

Limits:

- **Removal depends on the application calling `rotate`.** E2EE detects no
  access change (§3.5). Without a rotation after removing someone's read
  access, content written later stays under a key they hold; Jazz stops
  sending them rows, but a database leak would expose it to them.
- **Removal cannot erase copies.** A delivered key is never revoked. Keys,
  plaintext and ciphertext a former reader already obtained remain with them,
  and old versions stay readable to whoever holds their keys.
- **New readers get the full history.** The newest key opens every earlier
  version, so anyone granted read access to the space row can decrypt
  everything ever written in the space. Content that a later reader must not
  see belongs in another space.
- **The rotator is the only holder right after a rotation.** Readers obtain
  the new version by request (§3.4); until a device of the rotating account
  is online to answer, they keep reading old content and writing under the
  version they hold, and cannot read content written under the new one.
- **A space with nobody holding its current key is dead.** If every holder of
  the current version loses it before anyone else obtains it (for example the
  rotator loses its device without recovery), nobody can answer requests or
  rotate again (rotation needs the current key). Content of older versions
  stays readable to their holders; the application moves the content to a new
  space. The same holds after a rotation by an updater whose history is
  wrong (a client defect or bad faith; updaters are trusted to rotate).
- **The space row's read rule is the content's confidentiality rule.** Finer
  application rules inside a space are not cryptographically separated
  (§1.5). Read rules that use session claims are fine: a request is checked
  against the requester's own session.
- **Requests reveal readers to readers.** Key requests are readable by every
  reader of the space (R12): readers learn which accounts asked for which
  version and when, and so who else reads the space. A request by an account
  of generation above 0 also tells them that it reset (§2.11).
- **Space keys are shared by an account's devices.** Deliveries are sealed per
  account (§2.12): every active device of the account opens them, and device
  approval is the only per-device gate. A device removed by an account
  rotation keeps what it already opened.
- **No access check on answers.** The server does not re-check, when a
  delivery is written, that the requester can still read the space row
  (§3.4). An answer to a request accepted before a removal delivers only that
  older version.
- **Request IDs can be occupied.** The server does not check request ID
  derivations (§0). A reader can insert rows under another account's next
  request IDs; the victim steps to later attempts, and every such row names
  its author. A reader can also answer requests with keys that fail; the
  requester tries again. Both are visible abuse by a reader, which could
  disclose the keys anyway.
- **Equality tokens are linkable.** Tags are per key version, and a query
  sends one token per held version (§4.5), so the server can link the tokens
  of one value across versions, and sees how many rows of a column share a
  value within a version and which values queries look for (but not the
  values).
- **Writes with a held key are not freshness-checked.** A device holding an
  older version's key may write under it until it obtains a newer one or
  Jazz permissions refuse (§5.1).
- **App account roots are trusted on first use.** A local-first account's
  root is signed by the key its account ID derives from, so no one without
  the secret can forge or replace it, or reset the account (§2.3, §2.4),
  once a client has seen that signed root. A client meeting the account for
  the first time cannot tell from the account ID alone that it is
  local-first, so a server can show it an app-kind root instead; from the
  first signed root a client verifies, its floor and the no-downgrade rule
  protect it. An app account has nothing a client can check its root
  against: whoever can write as the account, the server included, can write
  its first root and sealing keys, and a server can show a different root
  (another device and key) to clients that have not seen the account before.
  Readers answering that account's requests then seal to that key. The
  account's own devices raise `root-not-mine` when they see a root other
  than the one they verified (§2.4), but only from their first observation
  on; other accounts have no way to tell. The SDK should present
  `root-not-mine` as "your encryption identity was replaced on the server;
  your data may be exposed", not as a sync error.
- **Rollback is detected only after a first observation.** A server can
  answer a read with an older cut, withholding rows accepted since (a device
  removal, a rotation, a retired recovery root), so that a client acts on
  stale state. Each device keeps floors (§6.3a) and refuses evidence older
  than what it has verified (`stale-state`); before its first observation of
  an account or space, and after a wipe, it cannot tell a rolled-back history
  from the real one. Other devices' floors do not protect it.
- **Account reset needs no approval.** For an application that enables
  `allowAccountReset`, whoever can sign in as an app-authenticated account
  can reset it and then request every space key it may read, with the full
  history (§2.11). Readers see a reset notice, after the fact. The account's
  own devices raise `device-no-longer-active` and must be approved again.
  Local-first accounts cannot reset: every generation's root must be signed
  by their identity (§2.4 step 1).
- **A second generation-0 root ends the account's E2EE.** Two different
  valid roots of generation 0 can only be written by the account's own
  sessions or by the server, since the server accepts at most one bootstrap;
  for a local-first account both must be signed by its identity. Clients
  fail closed with `conflicting-roots` (§2.4 step 1): every client refuses
  the account's state, at every later generation, so a reset does not
  escape it. The account is unusable for E2EE and the remedy is a new
  account. The SDK surfaces it loudly with its code, like `root-not-mine`.
  Whoever can write as an app account (or the server) can therefore deny it
  E2EE service this way, which gains it no content.
- **Removing a device of a local-first account does not exclude it.** For a
  local-first account, whoever holds the account secret is the account. A
  removed device that still holds the secret signs in as the account, keeps
  receiving its rows, re-enrols through recovery (§2.9) and decrypts. The
  remedy for a lost or stolen local-first device is a new account. Device
  removal remains effective for accounts authenticated by the application's
  own sign-in (§2.8).
- **Local-first recovery is as strong as the account secret.** The E2EE
  recovery key of a local-first account is derived from its account secret
  (§2.9), so whoever holds that secret (any device that stores it, a script
  on an origin that keeps it in `localStorage`, or anyone with its phrase or
  passkey) can restore the account's E2EE keys, and with them every space key
  sealed to the account. Passkey backups are protected by the platform's
  user verification.
- **Metadata is visible to the server and to readers permitted by policy:**
  which rows, spaces and devices exist; row IDs, relationships, space
  coordinates, authorship and timing; ciphertext sizes; key records and
  requests. The public account tables (R10) are readable by every signed-in
  session: anyone can see how many devices an account has enrolled and
  removed, when, and whether recovery is set up. The server stores and
  evaluates every E2EE row, so it sees who requests and answers which space
  keys.
- **No side-channel padding.** Ciphertext length reveals plaintext length.
- Local private records are protected only by the platform store; scripts on
  the same origin can read them.
- Integrity of encrypted cells is not checked by the server: Jazz stores any
  bytes a permitted writer submits; readers detect and leave out bad cells.
- **Internal records are never deleted** (R1). Requests and deliveries grow
  with readers and rotations, and a deleted space row or account leaves its
  records. This is accepted for the first release; the cost is to be measured
  with key-version growth.
