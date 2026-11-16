# E2EE protocol

This companion specifies the end-to-end encryption protocol: the identities,
the immutable records that carry keys and membership, how a client replays
them, and how reads, writes and private local state use the resulting keys.
It is the reference for a reimplementation and for its review. The byte formats
underneath are specified separately: [cryptographic formats](E2EE_CRYPTO_FORMAT.md)
(context, envelopes, cell cipher, key envelope, signatures),
[equality tokens](E2EE_EQUALITY_FORMAT.md) and
[large values](E2EE_LARGE_VALUE_FORMAT.md).
Schema changes that involve encrypted data are specified in [E2EE migrations](E2EE_MIGRATIONS.md).

It was derived from an earlier TypeScript implementation and its tests, and
then amended by design decisions. Where the two differ, this document states
the decided behaviour and notes briefly that the TypeScript implementation
behaved differently. Points that remain undecided are marked **Open question**.

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

- Every published public key (device, group epoch, recovery root) carries its
  mechanism ID and version (the `mechanism`/`version` and
  `signingMechanism`/`signingVersion` columns, or `env(mechanism, publicKey)`
  in signed bytes). Each identity has exactly one sealing key and one signing
  key, of one mechanism each, fixed when it is created: a device request, a
  recovery root and a group epoch's public-key record are immutable and never
  gain a second mechanism. Supporting a new mechanism means creating a new
  identity: a device enrols a new device ID (approved as in §2.5) and the old
  one is removed (§2.8); a recovery secret derives a second root for the new
  mechanism (§2.9); a group or space adopts it at its next rotation.
- Writers seal, sign and encrypt with the mechanism the application's
  configuration selects for the role. Sealing to a recipient that advertises
  no key of the selected mechanism fails with "recipient unsupported"; there
  is no fallback to another mechanism.
- Readers route by each envelope's mechanism and accept any registered one, so
  records written under an earlier selection stay readable.
- Migration: ship the new mechanism to every client, then switch the
  configuration. Each client then enrols a new device identity of the new
  mechanism and removes its old one; recovery secrets register their second
  root; the next rotation of each group publishes its new epoch's public keys
  with the new mechanism, and the next rotation of each space or group seals
  and wraps its new epoch key with it. Until a recipient has a key of the
  selected mechanism, sealing to it fails with "recipient unsupported" and the
  sender keeps the old configuration for that recipient's delivery pending.
  Old records are not rewritten.

### Identifiers

Every record ID, epoch ID, device ID and recovery root ID is a lowercase UUID
string, generated at random, except where a derivation is stated. Signed statements check IDs against
`^[0-9a-f]{8}-[0-9a-f]{4}-[47][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`
(versions 4 or 7). Account IDs are the authenticated Jazz account ID: any
non-empty string. A recovery root derived from a recovery secret (§2.9) has a
derived version 8 ID, and recovery root IDs accept versions 4, 7 and 8. The
space root ID is derived (§3.2) and is version 8, as are
the IDs of grants and group membership records staged by the recipient sync
or by a manager's self-enrolment (§3.8); those two record kinds accept
versions 4, 7 and 8. So are reset identities (§1.2), notices (§2.11) and
key-request answers (§3.4), which accept version 8 only.

Version 8 IDs come in two families, with different rules:

- **Public derived IDs** (space roots, grants and membership records staged
  by the sync or by self-enrolment, reset identities, notices, key-request
  answers). Each is formed the same way: the first 16 bytes of
  `SHA-256(UTF8(json([label, …])))`, with `b[6] = (b[6] & 0x0f) | 0x80` and
  `b[8] = (b[8] & 0x3f) | 0x80`, as a lowercase UUID; integers in the JSON (a
  count `n`, a `generation`) are decimal strings. The inputs are public row
  fields (and, for grants and membership records, a count of accepted rows,
  §3.8), so the server recomputes the derivation and refuses a row whose ID
  differs; nobody can occupy such an ID in advance.
- **Secret-derived recovery root IDs** (§2.9): keyed BLAKE2b from the
  recovery seed (which binds the account generation above 0), with the same
  version and variant bits. The server cannot
  recompute them; it checks only the version 8 form, R9, and that the ID is
  not already used. Only the account itself may register a root (R9), so a
  wrong ID can only break that account's own recovery, which the client
  detects by deriving again.

The crate pins one derivation fixture for each label
(`crates/jazz-e2ee/tests/protocol_vectors.rs`, cross-checked against
libsodium's SHA-256). With `S = 619ee262-ebb9-865c-a8db-26ede397c780` (the
first row) and `G = c3c3c3c3-c3c3-4c3c-8c3c-c3c3c3c3c3c3`:

| JSON input                                                                                                | Derived ID                             |
| --------------------------------------------------------------------------------------------------------- | -------------------------------------- |
| `["jazz.e2ee.space-id.v1","5c0e1d2a-3b4c-7d5e-8f60-718293a4b5c6","018f2e3d-4c5b-7a69-8877-665544332211"]` | `619ee262-ebb9-865c-a8db-26ede397c780` |
| `["jazz.e2ee.account-reset.v1","alice","1"]`                                                              | `0499408f-e71b-82e6-b00e-5cd5a54871e4` |
| `["jazz.e2ee.reset-notice.v1",S,"bob","2"]`                                                               | `3341a6cb-739f-86d7-b467-b7a6697ea872` |
| `["jazz.e2ee.admission-notice.v1",G,"bob","3"]`                                                           | `a3f2b36b-d96a-8cfd-8f7f-f76b551a701d` |
| `["jazz.e2ee.key-request-answer.v1","a1a1a1a1-a1a1-4a1a-9a1a-a1a1a1a1a1a1"]`                              | `7b2495e3-7747-819f-a09c-65567c132b7e` |
| `["jazz.e2ee.space-grant-id.v1",S,"manager","add","account","alice","0"]`                                 | `d0f306db-f7f6-81bc-bad7-c99ce5d5e1a2` |
| `["jazz.e2ee.group-membership-id.v1",G,"manager","add","account","alice","12"]`                           | `f2836b75-48c9-80bd-8306-e571d645fd42` |

The initial grant of a space (§3.3) is the `space-grant-id` row's form with
`role` `manager`, `operation` `add`, `recipientKind` `account`, the creator
and `n = 0`; the table's sixth row is the initial grant of a space `S`
created by `alice`. Secret-derived recovery root fixtures, for generations 0
and above, are in §2.9.

The same file pins the recovery derivation of §2.9, the group epoch key
pairs and the recipient commitment of §3.1, and the SHA-256 digest of every
record kind's signed bytes, each rebuilt there from this document's
formulas. Those digests pin this document's reading, not an
independent implementation's; the public approval, recovery root and
rotation request bytes are also built there byte by byte, without the
crate's encoders. A row that no derivation covers (for example a self-removal, R26) must use a version 4 or 7
ID. An insert refused because a row with that ID already exists is reported
to the writer exactly like a policy refusal, so a refusal does not reveal
whether a derived row exists.

Canonical JSON fields (`membership`, `revision`, `deliveries`) are parsed and
re-encoded; a field whose re-encoding is not byte-identical is rejected.

- **ID list** (`encodeEpochIds`): `json(sorted ids)`, strictly increasing, all
  UUIDs. A **revision** uses the same encoding but allows non-UUID strings.
- **Member map** (`encodeGroupMembership`): `json([[accountId, epochId], …])`
  sorted by account ID, strictly increasing, epoch IDs UUIDs.
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

| Records                                                                          | Domain                            |
| -------------------------------------------------------------------------------- | --------------------------------- |
| Content: cells, equality tokens, streams; group public keys; rotation requests   | `Dₛ`                              |
| Account ledger records of account `a` (devices, approvals, successors, recovery) | `Dₐ(a)`                           |
| Group records                                                                    | `Dₐ(group root accountId)`        |
| Space records (root, grants, successors, deliveries, markers)                    | `Dₐ(space root creatorAccountId)` |
| This device's private store scope                                                | `Dₐ(self)`                        |

A different signer, sender or recipient never changes a group's or space's
domain: it is always its creator's.

Space and group records are signed under their creator's account domain, as
the table above states; only content, group public-key records and rotation
requests use the shared domain. (A doc comment on the TypeScript `e2eeApplicationDomain` said
space and group records used the shared domain; the code bound them to the
creator's account domain, and that is the protocol.)

### What Jazz provides

The protocol is a client-side state machine over ordinary Jazz rows. It relies
on Jazz for:

1. **Authenticated authorship.** Every row's `$createdBy.account` is the
   authenticated account that wrote it, and policies can compare it.
2. **Policy enforcement** of the requirements in §1.4, at the authority.
3. **Ledger reads.** Inside a serializable transaction, a read of immutable
   rows returns, for each row, its accepted position (an unsigned 64-bit
   integer, decimal string at the boundary) and a common cut `through`. All
   reads of one evidence set must report the same cut; a client refuses
   evidence that spans cuts. Every returned row has exactly one position and no
   position exceeds the cut.
4. **Ledger transactions.** Inserts staged in one transaction become accepted
   atomically, at one position. Governance operations wait for Global
   acceptance (the authority's decision), not only Local.
5. **Read-set validation.** The authority refuses to commit a ledger
   transaction (with a conflict, not a policy refusal) when any evidence the
   transaction read at its cut has changed by its commit: a row it read was
   superseded in a way the read would have returned, or a row matching one of
   its reads was accepted after the cut. The protocol relies on this for one
   accepted successor per predecessor, one accepted row per derived ID (§3.8),
   recipient confirmation (§3.8) and the rotation delivery floor (§3.1).
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
  a device. An account has no keypair of its own.
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
- **Recovery root (recovery credential).** A key-free public record naming a
  recovery keypair (envelope + signing). The private half is derived from a
  **recovery secret** (§2.9): for a local-first account, the account secret
  itself; for an externally authenticated account, a separate secret backed up
  in a passkey. Exported **recovery material** holding the keypair directly is
  the fallback. A recovery root can receive the account epoch key and
  authorise a new device.
- **Group.** A set of accounts and child groups with its own epoch history,
  bound to one application row. Each group epoch has a 32-byte epoch key from
  which an HPKE (X25519) key pair and an Ed25519 key pair are derived; the public
  halves are published. Groups may nest.
- **Space.** The encryption scope of rows, for example a room. A space is named
  by its address `(scopeId, identifier)`: `scopeId` is Jazz's stable catalogue
  identity of the space table, `identifier` is the space row's ID. An
  encrypted table declares a `space` coordinate column (or `id`) whose value is
  the identifier; every row of a space is encrypted under the space's keys.
- **Space epoch and epoch key.** A space's key history is a chain of epochs:
  the root epoch, then one per **space successor** (rotation). Each epoch has a
  random 32-byte key.
- **Recipient.** An account or group granted a space. A member of a group
  recipient (directly or through nested groups) reaches the space key through
  the group's key, not through a delivery of its own.
- **Key manager / group manager.** An account Jazz lets manage a space's or
  group's keys (§1.4).
- **Grant.** A signed `add` or `remove` of a space recipient, in one of two
  **roles**: `manager` (the self-enrolment of a key manager, or the removal of
  that enrolment) or `member` (every other recipient change). Group membership
  records carry the same role.
- **Manager recipient.** An account whose latest accepted grant (for a group:
  membership record) of role `manager` naming it is an `add`, or the creator
  of the space or group while no accepted `manager` row names it (§3.8). The
  server and clients decide it from accepted rows alone (R25).
- **Delivery.** A space or group epoch key sealed to one device of a member
  account, or to a group recipient's epoch public key. A **recovery delivery**
  seals it to a recovery root instead. A **self-delivery** is one an account
  makes to its own devices.
- **Held key.** A space epoch key this device has authenticated and stores in
  its private store (§6). A held key is sufficient to read and write.

### 1.2 Account and device tables

All E2EE tables (this section and §1.3) are internal Jazz tables, like Jazz's
own account tables: they never appear in the application's `jazz.app.json` or
`jazz.permissions.json`, and the application cannot define or override their
policies. The tables in this section have fixed policies (§1.4). "Public" means readable by any session in `local-first` or
`external` auth mode; "private" means readable only by the row's
`$createdBy.account`. Every row is immutable.

**Column types** (this section and §1.3). IDs, account IDs, mechanism IDs
and the enumerated columns are strings; keys, envelopes, markers, signatures,
commitments, salts and canonical JSON columns (`membership`, `revision`,
`deliveries`) are bytes; `version`, `signingVersion` and `ledgerVersion` are
unsigned 32-bit integers (the common envelope's version width);
`generation` is an unsigned 64-bit integer, written as a decimal string
wherever it enters JSON. `covers` (§1.3) is text. Optional columns
(`recoveryRootId?`, …) are nullable.

| Table                              | Columns                                                                                                                                                                                   | Row ID          | Visibility | Meaning and writer                                                                                      |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------- | ---------- | ------------------------------------------------------------------------------------------------------- |
| `__e2ee_device_requests`           | `publicKey`, `mechanism`, `version`, `signingPublicKey`, `signingMechanism`, `signingVersion`                                                                                             | device ID       | private    | A device's identity, written by that device.                                                            |
| `__e2ee_device_keys`               | `deviceId` and the six key columns of the request                                                                                                                                         | random          | public     | Public projection of the request's keys; must equal it. Written by the device.                          |
| `__e2ee_account_identities`        | `ledgerVersion` (1), `generation`, `deviceId`, `epochId`, `envelope`, `verification`                                                                                                      | identity ID     | private    | Private account root: root epoch key sealed to the first device, plus its marker. Written at bootstrap. |
| `__e2ee_account_roots`             | `ledgerVersion` (1), `generation`, `accountId`, `deviceId`, `epochId`                                                                                                                     | random          | public     | Public account root. Written at bootstrap in the same transaction as the identity.                      |
| `__e2ee_device_challenges`         | `deviceId`, `epochId`, `envelope`                                                                                                                                                         | challenge ID    | private    | An approval challenge to a requesting device. Written by the approving device.                          |
| `__e2ee_device_proofs`             | `challengeId`, `proof`, `signature`                                                                                                                                                       | challenge ID    | private    | The requesting device's possession proof.                                                               |
| `__e2ee_device_approvals`          | `challengeId`, `signerId`, `verification`, `signature`                                                                                                                                    | challenge ID    | private    | Private approval with its account-key marker.                                                           |
| `__e2ee_device_deliveries`         | `challengeId`, `envelope`, `verification`                                                                                                                                                 | challenge ID    | private    | Account epoch key sealed to the approved device.                                                        |
| `__e2ee_public_device_approvals`   | `accountId`, `epochId`, `deviceId`, `signerId`, `signature`, `recoveryRootId?`, `recoverySignature?`                                                                                      | random          | public     | Key-free signed approval: the membership grant.                                                         |
| `__e2ee_account_successors`        | `accountId`, `predecessor`, `epochId`, `signerId`, `action`, `removedDeviceId?`, `retiredRecoveryRootId?`, `membership`, `revision`, `verification`, `history`, `deliveries`, `signature` | random          | private    | Private account rotation with the new key's deliveries.                                                 |
| `__e2ee_public_account_successors` | the private columns minus `verification`, `history`, `deliveries`                                                                                                                         | same as private | public     | Key-free signed rotation: the membership change.                                                        |
| `__e2ee_recovery_roots`            | `accountId`, `signerId`, `epochId`, `publicKey`, `mechanism`, `version`, `signingPublicKey`, `signingMechanism`, `signingVersion`, `signature`                                            | root ID         | public     | A registered recovery credential, signed by an active device.                                           |
| `__e2ee_recovery_deliveries`       | `rootId`, `epochId`, `envelope`                                                                                                                                                           | random          | private    | Account epoch key sealed to a recovery root.                                                            |

`action` is `remove-device` or `retire-recovery-root`; exactly the matching
target column is set.

The identity ID is the account ID for generation 0 (the first bootstrap). For
a reset (§2.11) it is the version 8 UUID formed, as for space IDs (§3.2), from
`SHA-256(UTF8(json(["jazz.e2ee.account-reset.v1", accountId, generation])))`,
with `generation` as a decimal string.

The TypeScript implementation also declared `__e2ee_recovery_protectors` and a
32-byte request `challenge` column; neither was ever read, and both are dropped.

### 1.3 Group and space tables

These tables are internal too. Their policies are derived by the server at
deploy time from the application's encryption declarations (§1.4); the
application writes no policy for them.

| Table                               | Columns                                                                                                                                                                                                                                                                    | Meaning and writer                                                                                                                                                                                                           |
| ----------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `__e2ee_groups`                     | `accountId` (creator), `deviceId`, `accountEpochId`, `epochId`, `mechanism`, `version`, `verification`, `signature`                                                                                                                                                        | Group root. Its ID is random and opaque (§3.1).                                                                                                                                                                              |
| `__e2ee_group_bindings`             | `groupId`, `scopeId`, `identifier`, `authorAccountId`, `authorDeviceId`, `authorEpochId`, `signature` (row ID = group ID)                                                                                                                                                  | Links a group to its governing application row. Readable only by the group's managers and current members, and by the managers R24 names. Written with the root.                                                             |
| `__e2ee_group_public_keys`          | `groupId`, `epochId`, `predecessorEpochId`, `proofEpochId`, `authorAccountId`, `authorDeviceId`, `authorEpochId`, `mechanism`, `version`, `publicKey`, `signingMechanism`, `signingVersion`, `signingPublicKey`, `keySignature`, `predecessorSignature`, `authorSignature` | Public half of a group epoch's key pairs, chained to the previous epoch's; decides the group's current epoch (§3.1). Same row ID as its root (the group ID) or successor. Written by a group manager with it.                |
| `__e2ee_group_membership`           | `groupId`, `epochId`, `authorAccountId`, `authorDeviceId`, `authorEpochId`, `operation` (`add`/`remove`), `role` (`manager`/`member`), `memberKind` (`account`/`group`), `memberId`, `signature`                                                                           | Signed membership change, by a group manager.                                                                                                                                                                                |
| `__e2ee_group_successors`           | `groupId`, `predecessor`, `proofEpochId`, `epochId`, `authorAccountId`, `authorDeviceId`, `authorEpochId`, `commitment`, `verification`, `history`, `authorEnvelope`, `signature`                                                                                          | Group rotation, by a group manager. `commitment` hides the member list (§3.1).                                                                                                                                               |
| `__e2ee_group_successor_recipients` | `groupId`, `salt`, `membership`, `revision` (row ID = successor ID)                                                                                                                                                                                                        | The member list behind a group successor's commitment. Manager-only.                                                                                                                                                         |
| `__e2ee_group_deliveries`           | `groupId`, `epochId`, `senderAccountId`, `senderDeviceId`, `senderEpochId`, `recipientAccountId`, `recipientDeviceId`, `recipientEpochId`, `envelope`, `signature`                                                                                                         | Group epoch key sealed to a member device (by a manager, or a self-delivery).                                                                                                                                                |
| `__e2ee_group_child_deliveries`     | `groupId` (parent), `epochId`, `senderAccountId`, `senderDeviceId`, `senderEpochId`, `recipientGroupId`, `recipientGroupEpochId`, `envelope`, `signature`                                                                                                                  | Parent group epoch key sealed to a child group's epoch public key.                                                                                                                                                           |
| `__e2ee_group_recovery_deliveries`  | `groupId`, `epochId`, `senderAccountId`, `senderDeviceId`, `senderEpochId`, `recipientAccountId`, `recoveryRootId`, `recipientEpochId`, `envelope`, `signature`                                                                                                            | Group epoch key sealed to a member's recovery root.                                                                                                                                                                          |
| `__e2ee_group_repairs`              | `groupId`, `epochId`, `deliveryId`, `accountId`, `deviceId`, `accountEpochId`, `signature`                                                                                                                                                                                 | A recipient's signed claim that a delivery to it is unusable; requests redelivery, grants nothing.                                                                                                                           |
| `__e2ee_rotation_requests`          | `sourceGroupId`, `sourceEpochId`, `causeKind` (`remove`/`rotation`/`coverage`), `causeId`, `covers`, `authorAccountId`, `authorDeviceId`, `authorEpochId`, `signature`                                                                                                     | Tells every group and space in which `sourceGroupId` is nested to rotate and not to deliver to keys from before the cause (§3.1). Written with a removal (by a manager, or by the removed member itself) or with a rotation. |
| `__e2ee_spaces`                     | `scopeId`, `identifier`, `creatorAccountId`, `deviceId`, `accountEpochId`, `epochId`, `initialGrantId`, `mechanism`, `version`, `verification`, `authorEnvelope`, `signature`                                                                                              | Space root; ID derived from the address (§3.2). Written by the creator's device.                                                                                                                                             |
| `__e2ee_space_grants`               | `spaceId`, `epochId`, `authorAccountId`, `authorDeviceId`, `authorEpochId`, `operation` (`add`/`remove`), `role` (`manager`/`member`), `recipientKind` (`account`/`group`), `recipientId`, `recipientEpochId`, `signature`                                                 | Signed recipient change, by a key manager.                                                                                                                                                                                   |
| `__e2ee_space_successors`           | `spaceId`, `predecessor`, `epochId`, `authorAccountId`, `authorDeviceId`, `authorEpochId`, `commitment`, `verification`, `history`, `authorEnvelope`, `signature`                                                                                                          | Space rotation, by a key manager. `commitment` hides the recipient list (§3.2).                                                                                                                                              |
| `__e2ee_space_successor_recipients` | `spaceId`, `salt`, `membership`, `revision` (row ID = successor ID)                                                                                                                                                                                                        | The recipient list behind a space successor's commitment. Manager-only.                                                                                                                                                      |
| `__e2ee_space_deliveries`           | `spaceId`, `epochId`, `senderAccountId`, `senderDeviceId`, `senderEpochId`, `recipientAccountId`, `recipientDeviceId`, `recipientEpochId`, `envelope`, `signature`                                                                                                         | Space epoch key sealed to one device of a direct recipient (by a manager, or a self-delivery).                                                                                                                               |
| `__e2ee_space_group_deliveries`     | `spaceId`, `epochId`, `senderAccountId`, `senderDeviceId`, `senderEpochId`, `recipientGroupId`, `recipientGroupEpochId`, `envelope`, `signature`                                                                                                                           | Space epoch key sealed to a group recipient's epoch public key.                                                                                                                                                              |
| `__e2ee_space_recovery_deliveries`  | as deliveries, with `recoveryRootId` instead of `recipientDeviceId`                                                                                                                                                                                                        | Space epoch key sealed to a direct recipient's recovery root.                                                                                                                                                                |
| `__e2ee_space_notices`              | `spaceId`, `epochId`, `kind` (`reset-approved`/`admitted`), `accountId`, `generation`, `authorAccountId`, `authorDeviceId`, `authorEpochId`, `signature`                                                                                                                   | A key manager's signed approval of a recipient account's reset, or record of an account admitted at a generation above 0; tells the space's recipients (§2.11).                                                              |
| `__e2ee_group_notices`              | `groupId`, `epochId`, `kind` (`reset-approved`/`admitted`), `accountId`, `generation`, `authorAccountId`, `authorDeviceId`, `authorEpochId`, `signature`                                                                                                                   | The same for a group, by a group manager.                                                                                                                                                                                    |
| `__e2ee_key_requests`               | `scopeKind` (`space`/`group`), `scopeId`, `epochId`, `accountId`, `deviceId`, `accountEpochId`, `signature`                                                                                                                                                                | A manager recipient's signed request for the current key it lacks; lets a key-holding recipient restore it (R27, §3.4).                                                                                                      |
| `__e2ee_space_repairs`              | `spaceId`, `epochId`, `deliveryKind` (`device`/`recovery`), `deliveryId`, `accountId`, `deviceId`, `accountEpochId`, `signature`                                                                                                                                           | A recipient's signed claim that a space delivery to it is unusable; requests redelivery, grants nothing.                                                                                                                     |

Each encrypted application table stores every encrypted column as non-null
`bytes` (ciphertext; a null value is encrypted as a null value), plus a
non-null `bytes` column `__e2ee_eq_<column>` for each equality-indexed column.
Two short values are never ciphertext and have fixed meanings (§4.1): the
empty byte string (never written) and the 5-byte cleared marker
`4a 45 32 58 01` (written by a schema conversion, [migrations](E2EE_MIGRATIONS.md) §3.2).

### 1.4 Permissions

Jazz is the only permission authority. E2EE adds no permission logic of its
own: replay authenticates signatures, device activity and epochs, but it does
not decide who may administer a space or group, and possession of a key grants
no Jazz permission. The policies below are what Jazz enforces on the internal
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
  of the generation before it, the session is not `local-first`, and the
  deployed encryption configuration sets `allowAccountReset` (§2.11); read by
  that account.
- R6. `__e2ee_account_roots`: insert only with `accountId = session account`
  and a matching identity (identity ID of that generation, same device, epoch,
  generation and ledger version) by the same account, in the same
  transaction.
- R7. Challenges, proofs, approvals, device deliveries: insert by `$createdBy`
  only, referencing a request or challenge of the same account; read by
  creator.
- R8. Public device approvals: `accountId = session account`; the signer's
  request and the approved device's request belong to that account; a
  recovery approval's `recoveryRootId` names a recovery root of that account.
  Both successor tables: `accountId = session account`; the signer request
  and the target (removed device's request or retired recovery root) belong
  to that account; exactly one target column is set, matching `action`; the
  private and public rows share their ID and are inserted in one
  transaction.
- R9. Recovery roots: `accountId = session account`, signer request of the
  same account. Recovery deliveries: the root's `accountId` is the session
  account; read by creator.
- R10. Public tables (roots, device keys, public approvals, public successors,
  recovery roots) readable by every `local-first` or `external` session, so
  other accounts can replay a member's account history.

Space tables (policies derived at deploy). Each encrypted table's `encryption`
entry names its space table (`scope`). For every space table `T`, the server
derives policies over the space rows whose `scopeId` is `T`'s catalogue
identity; the governing row is the row of `T` whose ID is the space's
`identifier`. An application with several space tables gets independent
derived policies for each.

- R11. **Reads are self-only.** A **current recipient** of a space may read
  the space root row, its reset approvals (`__e2ee_space_notices`, §2.11) and
  the key-version chain (`__e2ee_space_successors`,
  needed to know the current epoch and to authenticate its key back to the
  root), and the space's key requests (R27). **Current recipient** and
  **current member** are defined once, per role: for each role (`manager`,
  `member`) the latest accepted row of that role naming the recipient (by
  position, then ID) decides that role's state, and the creator holds the
  `manager` role until a `manager`-role row names it; the recipient is
  current iff either role is `add`. A current recipient is an account that is
  current in this sense on the space (a direct recipient), or a current
  member, directly or through nested groups, of a group that is current on
  the space; devices of such an account read as the account. R17, R22, R27,
  §3.4 and §3.5 use the same definition; a `remove` of one role leaves the
  other role's state unchanged. Any account named by a grant, current or
  former, may read the grants naming it and the deliveries and recovery
  deliveries addressed to it (`recipientAccountId` = its account); members of
  a group recipient may read the grants naming that group and the group
  deliveries addressed to it. A removed recipient thus keeps reading its own
  `remove` grant, but loses the root and the chain. The server decides
  "latest grant" over accepted rows; it cannot check signatures, so a row that
  client replay rejects (bad signature, wrong epoch, inactive author device)
  still changes server-side read access. Client replay decides only which
  keys and recipients are valid.
- R12. A **key manager** of the space (the R13 right) may read every space
  record of that space, including `__e2ee_space_successor_recipients`. This is
  required to rotate, to deliver to every remaining recipient and to add
  recipients at the current epoch. Every key manager is also a recipient
  (§3.8), so it holds the current key and reads the space's content.
- Nobody else reads any space record. There is no read-all option: an
  application builds its member lists from its own membership tables, never
  from grants.
- R13. **Insert** into `__e2ee_spaces` (create a space), `__e2ee_space_grants`
  (change recipients), `__e2ee_space_successors` and
  `__e2ee_space_successor_recipients` (rotate), and the three space delivery
  tables (deliver keys), and `__e2ee_space_notices`, iff, apart from the
  exceptions R21, R22, R26 and R27 state, the session passes the space's
  declared
  **`manageKeys`** check on the governing row (§1.5): Jazz lets it perform the
  named operation (`read`, `insert`, `update` or `delete`) on the governing
  row, or on the row reached from it through the named forward relation.
  `manageKeys` is required; there is no implicit default.
- R14. Grant `recipientKind` and `operation` take only their defined values.
  A successor's recipients row has the successor's ID and is inserted in the
  same transaction as the successor.
- R15. Ordinary application tables keep their own read and write policies.
  When a space table declares a membership relation (`recipients`, §1.5), the
  application's read policy on the membership table must let every key
  manager of a space read every row of that relation for the space; the
  recipient sync (§3.8) sees only what the manager can read. Jazz cannot prove
  this at deploy. The simplest policy that satisfies it repeats the
  `manageKeys` check, for example `allowedTo.update(room)` on a `roomMembers`
  row when `manageKeys` is `update` on the room. A manager that sees only part
  of the relation fails safe: it never removes a recipient it cannot confirm
  is gone (§3.8).
  There is no separate E2EE administration authority: whoever passes the
  `manageKeys` check is a key manager of the space, and nobody else. A key
  manager is always a recipient as well (§3.8): it enrols itself, receives every
  epoch key and can therefore read the content it manages. So a manager can
  always deliver the current key to a new member, and adding a member never
  requires a rotation. An application whose backend acts as a key manager gives
  that backend the keys, which defeats end-to-end encryption for those spaces;
  keep `manageKeys` to accounts of people. Which
  accounts may manage keys is decided by Jazz permissions on the server; clients
  verify authorship and signatures, not authorisation. A space created together
  with its governing row is checked against the row as the transaction leaves
  it.

When the space table declares `recipients` (§1.5), the space's recipient list
follows the application's membership relation automatically (§3.8): the
application never adds or removes recipients itself, and the two lists
converge as soon as a key manager's client is online. Until then a new member
sees its key as pending (§4.3). Each recipient sees its own status from its
own grants and deliveries: not granted, granted with a delivery pending, or
holding the key. Without `recipients`, the application adds and removes
recipients explicitly, and keeps them in step with its own member list.

The TypeScript implementation behaved differently: the space and group tables
were declared in the application schema, denied by default, and the
application hand-wrote their policies (the BandChat example's
`permissions.ts`); only one space table per application was supported.

Group tables (policies derived at deploy). A group is a nested recipient set
(for example a team added to a room) and is bound to one application row, the
**governing row** of the group (for example a `teams` row). The group's ID is
random; the link to its governing row is the group's `__e2ee_group_bindings`
row (`scopeId`, `identifier`). The server, which sees every row, evaluates the
derived group policies through that link. The group's table is declared in
the app file (§1.5). The rules mirror the space rules:

- R16. **Group manager** = the session passes the group declaration's
  required `manageKeys` check on the group's governing row (§1.5), exactly as
  for spaces (R13). A group manager is always a member of the group as well
  (§3.8). A group manager may
  create the group root, publish its public keys, approve resets,
  insert membership records
  (adding and removing accounts and child groups), rotate (successor and
  recipients rows) and deliver group keys. Creating a group inserts its root,
  binding and first public-key record together; a binding is accepted only if
  the session manages the named governing row and no other binding names the
  same row (one group per row; the server keeps `(scopeId, identifier)` of
  bindings unique). A public-key record is accepted only in the transaction
  of its group's root (row ID = the group ID, empty `predecessorEpochId` and
  `proofEpochId`, the root's epoch) or of a group successor (row ID = the
  successor's row ID, the successor's predecessor, proof epoch and new epoch,
  the same author), with `authorAccountId` the session account, and only one
  per `(groupId, epochId)`. So a successor and its public-key record are one
  event with one ID (§3.1).
- R17. **Reads are self-only plus managers.** A current member account
  (current per role as in R11, directly or through a nested child group; or a
  device of such an account) may read the group root, its binding, its reset
  approvals (`__e2ee_group_notices`), its key requests (R27) and the
  key-version chain (`__e2ee_group_successors`). Any account or group
  named by a membership record may read the records naming it; an account may
  read deliveries and recovery deliveries addressed to it; members of a child
  group may read the child deliveries addressed to that child. A group manager
  may read every record of the group. Nobody else reads any of it.
- R18. `__e2ee_group_public_keys` is readable by every `local-first` or
  `external` session, like device public keys. It carries the random group
  ID, the epoch IDs of the chain, the public keys and the author (account,
  device and account epoch) of each epoch, so it reveals which account
  published each epoch (a manager of the group, §3.5, §8), but nothing about
  which application row the group belongs to or who its members are.
- R19. A member may insert a group repair, and a recipient a space repair,
  for a delivery addressed to itself (`accountId` = session account).
- R20. **Nesting and group recipients.** Adding or removing child group `C` in
  group `P` requires only the manager right on `P`; granting or removing group
  `G` on a space requires only the space's management right (R13). No right on
  the child's governing row is needed and the child's members are never read:
  the manager seals the parent key to the child's current public key. The
  server refuses (derived policy, evaluated over the full graph) any `add` of
  a child group, or grant of a group on a space, that would create a cycle or
  a chain of more than 8 nested groups.
- R21. **Re-wrapping after a child rotates.** A manager of group `C` may insert
  a space group delivery or a group child delivery addressed to `C`
  (`recipientGroupId = C`) without any right on the space or parent, iff a
  delivery of the same space or parent and epoch to `C` already exists. This
  lets `C`'s managers re-wrap every key `C` holds for `C`'s new epoch.
- R22. **Self-delivery.** An account may insert a `__e2ee_space_deliveries` or
  `__e2ee_group_deliveries` row without any management right iff
  `senderAccountId = recipientAccountId =` session account, the target
  device's request and key rows were created by that account, and the account
  may read the space or group under R11 or R17. This covers delivering keys
  it already holds (including keys from its own recovery deliveries) to its
  own approved devices. The server checks device ownership only; whether the
  device is active is checked by clients when they replay the account (§2.4).
  The server cannot check that the sending device holds a key or is active;
  this is harmless, because an account can only deliver to its own devices,
  and a bad self-delivery is skipped by the recipient's verification (§3.5).
- R23. **Rotation requests.** A manager of group `C`, or a member removing
  itself from `C` (R26), may insert a `__e2ee_rotation_requests` row with
  `sourceGroupId = C` in the same transaction as a `remove` membership record
  of `C` (`causeKind` `remove`, `causeId` = that record, empty
  `sourceEpochId`, empty `covers`); a manager of `C` also with a successor of
  `C` (`causeKind` `rotation`, `causeId` = the successor, `sourceEpochId` =
  its new epoch); and a manager of `C` alone, naming `C`'s current epoch
  (`causeKind` `coverage`, §3.1). A manager of any group, and a key manager
  of any space, in which `C` is currently nested (directly or through other
  groups) may read it, for as long as the request exists (§3.1), together
  with its paths: the direct children or group recipients of the ancestor
  through which `C` is reached, which the server computes at read time from
  the current-edge projection. The server evaluates the nesting over the
  current graph. The writer never learns the ancestors or their members.
- R24. **Resolving a declared group recipient.** When a space table's or
  group table's `recipients` declaration names a `group` column (§1.5), a
  manager of a space (or parent group) may read the `__e2ee_group_bindings`
  row whose governing row is named by the `group` column of a row of that
  space's (or group's) membership relation. This is how the recipient sync
  (§3.8) learns the group ID to grant or add; the group's public keys are
  already public (R18). The manager reads nothing else of the group: not its
  members and not its other records.
- R25. **Last manager.** The server refuses with `last-manager` a transaction
  whose space grants (or group membership records) would leave a space (or
  group) with no manager recipient. It evaluates the transaction as a whole:
  it applies every grant of the transaction to the accepted rows, in
  position-then-ID order, and refuses if no account is then a manager
  recipient. A **current manager recipient** is decided from accepted rows
  alone, by the server and clients alike: for each account, the latest
  accepted grant (membership record) of role `manager` naming it (by
  position, then ID) decides, and an `add` makes it a manager recipient; the
  creator counts as one until a `manager`-role row names it. A historical
  `add` followed by that account's `remove` does not count, and two removals
  in one transaction are checked against the state after both. Rows that
  client replay would reject count the same as valid ones, as for R11.

  This is the only server enforcement of the rule in the first release. The
  server does not check application writes: a write to the governing row, to
  the row `manageKeys.via` reaches, or to any other input of the
  `manageKeys` rule that takes the right from every manager recipient is
  accepted, and so is a change outside Jazz (a role in an external token, a
  change in the identity provider). The SDK refuses, with `last-manager`,
  leaving, giving up the manager role and demoting itself where it can
  recognise that it is the last manager recipient; application data edits can
  still orphan a space or group, which then keeps its recipients and keys but
  cannot add members or rotate until someone gains the right and enrols.
  Enforcing the rule on the post-state of application transactions (mapping
  written rows back to the spaces and groups whose `manageKeys` they feed,
  over both the old and new row images) is possible later hardening work.

- R26. **Self-removal.** A recipient may insert a `remove` grant (a member, a
  group `remove` membership record) naming its own account, authored by
  itself, without a management right, with a version 4 or 7 ID; R25 still
  applies. Of role `manager`, it gives up the manager enrolment. Of role
  `member`, it is refused while the space (or group) declares `recipients` and
  a row of the membership relation still names the account: leaving is then
  deleting that row, and the sync removes the recipient (§3.8). A member
  leaving a group writes the rotation request of R23 in the same transaction,
  as a manager's removal does; the runtime refuses a group self-removal
  without it.

- R27. **Key requests and delivery to a manager recipient.** A current
  manager recipient may insert a `__e2ee_key_requests` row for the current
  epoch of a space (or group), naming its own account and one of its own
  devices (`accountId` = session account, device request created by it). Any
  current recipient (member) may read the key requests of that space (group).
  A current recipient account may then insert a space delivery (group
  delivery) without the management right iff its row ID is the answer ID of
  an accepted key request (§3.4) of the same space and epoch, it is addressed
  to the device that request names, and that account is still a current
  manager recipient and not reset-pending (§2.11). So each request admits
  exactly one answer. A bad delivery is skipped by
  the recipient's verification (§3.5). Who answers, and the reset check, are
  in §3.4.

R11, R17, R22, R23 and R27 require the server to evaluate membership through
nested groups (bounded by the depth limit of 8) in read and insert policies.

Which accounts manage a group is decided by Jazz permissions on the server;
clients verify authorship and signatures, not authorisation. (The TypeScript
implementation had no governing row for a group: its ID was chosen by the
creator and administration was an application-written policy over the group
root's `accountId`. Nested adds also required administering the child, every
space delivery went to the devices of every account reachable through groups,
and there was no self-delivery.)

### 1.5 App-file declarations

Space tables need no declaration of their own: an encrypted table's
`encryption` entry names its space table in `scope`, and the space policies
are derived for that table. The entry also says who manages the space's keys
(`manageKeys`, required) and, optionally, where the space's membership lives
(`recipients`):

```json
"rooms": {
  "columns": { … },
  "relations": { "members": { "reverse": "roomMembers", "relation": "room" } }
},
"roomMembers": {
  "columns": { "roomId": "uuid", "accountId": "string?", "teamId": "uuid?" },
  "relations": { "room": { "rel": "rooms", "column": "roomId" },
                 "team": { "rel": "teams", "column": "teamId" } }
},
"messages": {
  "columns": { … },
  "encryption": {
    "space": "roomId", "scope": "rooms",
    "columns": { "body": "string" }, "equality": ["body"],
    "manageKeys": { "allowedTo": "update" },
    "recipients": { "via": "members", "account": "accountId", "group": "teamId" }
  }
}
```

- **`manageKeys`** (required) has an optional `via`, naming a forward
  relation of the space table, and `allowedTo`, one of `read`, `insert`,
  `update` or `delete`. A session manages the space's
  keys iff Jazz lets it perform `allowedTo` on the space row, or, with `via`,
  on the row the space row's forward relation `via` points to; this is the
  permissions-file atom `allowedTo.<op>(<via>)` evaluated on the space row.
  The deploy and code generation refuse an `encryption` entry without it, for
  example: "`messages.encryption` needs `manageKeys`, which says who manages
  the keys of `rooms` spaces. To let whoever may update the room manage them,
  add `"manageKeys": { "allowedTo": "update" }`."
- A `manageKeys` rule that reads session claims (directly or through the
  policies it inherits) cannot be evaluated for another account (recipient
  confirmation answers `unknown`, §3.8), so a manager demoted only in the
  claims is never removed automatically. The deploy refuses such a rule unless
  the declaration acknowledges it with `"claims": "acknowledged"`; managers of
  such spaces then remove former managers explicitly (§3.8). The deploy also
  warns when the rule's operation is `read`, or when the rule admits many
  accounts, because every account that passes it becomes a manager recipient
  and receives the keys.
- **`recipients`** (optional) has `via` and at least one of `account` and
  `group`. `via` names a reverse relation declared on the space table; its
  source is the **membership table**. `account` names a `string` or `uuid`
  column of the membership table holding a member's Jazz account ID. `group`
  names a reference column of the membership table pointing at a table
  declared with `encryptionGroup`; the member is that row's group. A
  membership row names exactly one recipient: its `account` when that is
  non-null, otherwise its group; a row with both null names none. Several rows
  naming the same recipient count once. The deploy refuses a `via` that is not
  a reverse relation of the scope table, a missing or mistyped column, or a
  `group` column whose target table has no `encryptionGroup`.
- **Column entries.** A `columns` value is either a type string (`"body":
"string"`) or, during a schema conversion, an object with `type` and the
  conversion fields of [migrations](E2EE_MIGRATIONS.md) §3.2 (`from`, `to`,
  `convert`, `sealedType`); the string is shorthand for `{ "type": … }`.
- **Manual mode.** An entry without `recipients` leaves recipients to the
  application, which adds and removes them explicitly (§3.4). Use it only when
  membership is not a simple relation of the space row.
- Every `encryption` entry with the same `scope` must state the same
  `manageKeys` and the same `recipients` (or all omit `recipients`); the
  deploy refuses a disagreement.

A group table is declared with one additional table key. Its `manageKeys` is
required and its `recipients` optional, with the same shapes and meanings,
evaluated on the group's governing row; the `recipients` relation lists the
group's members, and its `group` column names child groups:

```json
"teams": {
  "columns": { … },
  "relations": { "members": { "reverse": "teamMembers", "relation": "team" } },
  "encryptionGroup": {
    "manageKeys": { "allowedTo": "update" },
    "recipients": { "via": "members", "account": "accountId", "group": "subteamId" }
  }
}
```

An empty `encryptionGroup` is refused with the same message as a missing
`manageKeys` in an `encryption` entry. Without `recipients`, group members are
added and removed explicitly (§3.1); with it, explicit changes are refused
with `recipients-declared`. Like the `encryption` key, `encryptionGroup` is
client and deploy metadata: it adds no stored column and changes no schema
hash.

Application-wide encryption settings live in an optional top-level
`encryption` object of `jazz.app.json`, beside `tables`; it is client and
deploy metadata too, outside the schema hash, carried in the deploy request.
Its first field is `allowAccountReset` (boolean, default `false`, §2.11):

```json
{ "encryption": { "allowAccountReset": true }, "tables": { … } }
```

Documentation should recommend at least two key managers for every space and
group. A space or group whose managers are all gone cannot add members or
rotate; the server therefore refuses removing the last manager recipient's
grant (R25), but not an application write that takes the right away.

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
- public root `{accountId, generation: 0, deviceId, epochId, ledgerVersion: 1}`,

where `A(recipient, column) = ctx{application: D, policy:
"jazz.e2ee.account-identity.v1", scope: "account", identifier: account,
table: "__e2ee_account_identities", row: account, column, epoch: epochId,
recipient}`. Both envelopes are opened again before submission. The root is
not signed: its authority is `$createdBy` plus R5–R6. At most one concurrent
bootstrap is accepted.

### 2.4 Account replay

Replay of an account (`replayOrderedAccount`) is over public rows at one cut:

1. **Root.** Let `g` be the highest `generation` among the account's public
   roots. The canonical root is the earliest public root of generation `g`,
   ties by smallest ID; every row before it (and every row of an earlier
   generation) is ignored for the current state (§2.11). A generation `g > 0`
   counts only if a root of generation `g − 1` was accepted before it;
   otherwise those roots are ignored. Any root with `ledgerVersion ≠ 1` is an
   unsupported capability. Any other root of generation `g` that differs in
   account, device or epoch is an intrinsic contradiction
   (`conflicting-roots`). Conflicting device-key rows, or a root device without
   keys, are intrinsic contradictions.
2. **Grants within an epoch.** Public approvals of the current epoch strictly
   after the epoch's position are applied in batches of equal position. An
   approval is accepted iff its signer was active _before the batch_, its
   recipient is not removed, its signature over the public approval bytes
   (§2.5) verifies, and it is not a non-recovery approval carrying a
   `recoverySignature`. A recovery approval (§2.9) is accepted regardless of
   the signer's membership. A false verification skips the row; a signer
   failure propagates. A missing recipient key is an intrinsic contradiction.
3. **Successors.** Visited in position order. A candidate is skipped if its
   account or predecessor differs, it is not strictly after the current epoch
   position, its epoch was already used, **another successor of the same
   predecessor or any recovery root has the same position**, it is malformed,
   its public `revision` differs from the recomputed one, its signer was not
   active before it, its target is not an active device (removal) or not a
   valid, earlier, unretired recovery root (retirement), its signature fails,
   or its `membership` is not exactly the active set minus the target. The
   first accepted successor of the current epoch advances the epoch.
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

**Private verification.** A device that wants the account key also reads the
private tables (same cut) and accepts: the identity only if it matches the
public root (same generation, device, epoch and ledger version) at exactly
the same position; a private approval only if its
challenge and proof were accepted at or before it, a public approval with the
same `(account, epoch, signer, device, position)` exists, request and key
projections match at its position, both the proof signature and the approval
signature verify; a private successor only if the accepted public successor
with the same ID is at the same position and has the same `accountId`,
`predecessor`, `epochId`, `signerId`, `action`, target column and
`membership`, its signer's request matches, every ID of its private
`revision` is a verified private approval of the predecessor epoch before it,
and its own signature verifies. The two `revision` fields differ by design
(private approval IDs and public approval IDs, §2.8) and each is checked, with
its signature, in its own domain.

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
   - public approval `{accountId, epochId, deviceId, signerId, signature}`
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
Space and group keys reach the new device afterwards by self-delivery from the
account's other devices (§3.6), with no key manager involved.
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
  `S("signature", signerId) ‖ frame[predecessor, action, targetId, membership,
revision, verification, history, deliveries]`;
- public successor (same ID): same `membership`; `revision` = revision of the
  IDs of the public approval rows of the predecessor epoch that public replay
  accepted (§2.4, step 2) before the successor's position; signature
  over `ctx{application: D, policy: "jazz.e2ee.public-account-successor.v1",
scope: "account", identifier: account, table:
"__e2ee_public_account_successors", row: id, column: "signature", epoch:
newEpoch, recipient: signerId} ‖ frame[predecessor, action, targetId,
membership, revision]`;
- a recovery delivery of `K'` for every unretired active recovery root whose
  sealing key is of the selected sealing mechanism; a root of another
  mechanism gets none (§0, mechanism agility) and does not block the
  rotation.

Removal does not retire recovery roots; retiring a root does not remove
devices. Only one rotation from a given predecessor can win.

**What removal achieves depends on how the account signs in.** For an
account authenticated by the application's own sign-in, a removed device can
no longer obtain keys: it is not active, receives no delivery of the new
account key and cannot approve itself, and the recovery secret is separate
from anything the device stores. For a **local-first account**, whoever holds
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
  `BLAKE2b-256(key = seed, input = UTF8("jazz.e2ee.recovery-kem-key.v1"))`,
  the signing key pair's Ed25519 seed is
  `BLAKE2b-256(key = seed, input = UTF8("jazz.e2ee.recovery-sign-key.v1"))`,
  and the root ID is the first 16 bytes of
  `BLAKE2b-256(key = seed, input = UTF8("jazz.e2ee.recovery-root-id.v1"))`
  with the version and variant bits set as for space IDs (§3.2). The labels
  are bound to the current mechanisms (`jazz.wrap.hpke-x25519` v1 and
  `jazz.sodium.sign` v1); a later mechanism defines its own labels and gives a
  second root, registered alongside. The `0x00` separates the label from the
  domain, and including `Dₐ` gives different roots per application and
  environment from one secret. The derivation is pure: the same `s` and
  account always give the same root. Fixture: `s` = bytes `00`–`1f` and
  `Dₐ = ["0f8e3a4c-1d2b-4c5d-8e6f-7a8b9c0d1e2f","prod","alice"]` give root ID
  `032f45bb-7f73-8d44-8896-7742cf0aafa6`, sealing public key
  `9519969c333c52d91aee8627c00a9cb2b1e879b985a451cdcfc57c93dfbf2c06` and
  signing public key
  `a90b937a1cae643ec7a3d1c7b96264f6ff761bed0d44fdef086be8a350211cf7`; as
  text, `s` is `jazz-auth-v1:AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8`.
  The same `s` and `Dₐ` give root ID `6eee5902-eeee-87d2-befc-f93f8ef1d5bc`
  at generation 1 and `675d3f81-0e2f-87aa-b146-cb2e632bff7f` at
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
  with a supported signing mechanism, and its signature verifies. Later removal
  of the registering device does not invalidate it.
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
deploy request). It is never
available to a local-first account: there the account secret is the account,
and losing it is losing the account. The server enforces both: R5 refuses an
identity of generation above 0 from a local-first session, or when the
deployed configuration does not allow reset.

Stage `account-reset`. Preconditions, read at one cut: the account has a
canonical root of generation `g`; this device has a request whose keys match
it (as for bootstrap: `no-device-request`, `device-keys-mismatch`) and is not
an active device of generation `g` (`reset-device-active`: an active device
can approve new devices, so it never needs a reset). Then insert, exactly as a
bootstrap but with `generation: g + 1` and the reset identity ID (§1.2), an
identity and a public root. The contexts are those of §2.3 with `row` the
identity ID. At most one reset per generation is accepted (§0 item 5).

Effects, by replay (§2.4): the new root starts the account's history again.
Devices, approvals, successors and recovery roots of earlier generations are
inactive; a recovery root must be registered again: from the same recovery
secret, the root derived for the new generation (§2.9). Records the account
signed before the reset stay valid (state as of a position, §2.4).

**Reset accounts wait for a manager's approval.** A reset lets whoever can
sign in as the account ask for keys, so it removes the protection that
device approval otherwise gives against a stolen sign-in. The check that
replaces it is a key manager's explicit approval, in each space and group
separately:

- **Admitted generation.** For each account and space (group), the
  **admitted generation** is set when the account first enters: for the
  creator, by the root itself, as the generation of the root's author epoch
  (the space root's `accountEpochId`, the group root's `accountEpochId`, as of
  the root's position, §2.4), whether or not a grant or membership record
  also names it; for everyone else, at its first accepted `add` there (of
  either role, bound to an account epoch of that generation, account state as
  of the grant's position). There is no generation-0 default: an account
  with no admitted generation has never entered. It is raised to `g` by each
  valid approval of generation `g` there (below). Nothing else changes it:
  removing and re-adding, re-enrolling, changing roles, or any `add` re-binding
  a newer account epoch without an approval leaves it as it was.
- **Pending.** An account whose current generation is newer than its admitted
  generation in a space (group) is **reset-pending** there, whatever its
  latest grants say. The state is decided from rows (account roots with their
  generations, the account's earlier grants or membership records, and the
  approvals), so the server and clients agree on it.
- **A pending account cannot govern there.** While an account is
  reset-pending in a space (group), the server refuses every governance
  insert it makes there: grants and membership records (including
  self-enrolment and self-removal), successors and recipients rows,
  deliveries (including self-deliveries, R22, and key-request answers, R27),
  public-key records, rotation requests, notices and approvals, and key
  requests. Client replay ignores such records by the **pending-author rule**
  of the chain rule (§3.1), stated in notice form so that every client can
  check it, so a raw row the server should have refused changes nothing a
  client accepts. A pending
  manager therefore cannot act as a manager until another manager approves
  it; the admission and approval rules apply to both roles and to group
  membership alike.
- **Excluded, not stale forever.** The reset changes the account epoch, so
  the space (group) needs one rotation; that rotation and every later
  delivery leave the pending account out: it is not in the rotation's
  `membership`, receives no delivery, and the recipient sync neither
  re-grants nor delivers to it (§3.8). Once a rotation accepted after the
  account's reset root has excluded it, the pending account no longer makes
  the space (group) stale; it does again only after a further reset. It stays
  a recipient (its grants are unchanged), and its reads report the space
  `key-pending`.
- **Where managers see it.** Managers replay every direct recipient's public
  account history (R10, R12), so their runtime lists the reset-pending
  accounts of every space and group they manage. The SDK surfaces them as a
  `reset-approval-needed` encryption event `{account, space or group,
generation}` and in `db.encryption.pendingResets()`, so the application can
  ask a manager "Bob reset his encryption — approve?". Approving is an
  application action (`db.encryption.approveReset(space or group,
account)`); it is never automatic.
- **Approval.** A key manager other than the reset account approves in one
  publication: a **reset approval** row (`__e2ee_space_notices` or
  `__e2ee_group_notices`, `kind` `reset-approved`) `{spaceId (groupId),
epochId, kind, accountId, generation, authorAccountId, authorDeviceId,
authorEpochId, signature}`, signed by the author device over
  `X(json(["notice", id, kind, authorAccountId, authorDeviceId,
authorEpochId]), json([accountId, generation]), epochId)` (for a group, `G`
  with the same column and recipient), with the version 8 ID derived (§0)
  from `json(["jazz.e2ee.reset-notice.v1", spaceId (groupId), accountId,
generation])`. A reset approval names a generation above 0; one of
  generation 0 is malformed (no reset reaches it), and so is one whose
  `authorAccountId` is the approved `accountId`. The approval carries an `add` grant (membership record) of each role the account
  holds, binding its new account epoch; and deliveries of the current epoch
  key to its active devices and recovery roots. Replay applies such a
  re-binding `add` without requiring rotation when a valid approval of that
  account and generation is at the same position (§3.4). The server admits
  the approval under R13 (R16) and refuses one whose author is the approved
  account or is itself reset-pending there. Client replay accepts an
  approval only from a manager recipient that is not reset-pending at its
  position.
- **Admission notice.** Members cannot read other recipients' grants, so they
  learn an account's admitted generation from notices. When a manager first
  admits an account whose current generation is above 0 to a space (group)
  (a grant by the sync or by hand, or the account's own self-enrolment by an
  account that was never a recipient there), the same publication writes a
  notice of `kind` `admitted` with that generation, in the same table, with
  the same signed bytes and the version 8 ID derived from
  `json(["jazz.e2ee.admission-notice.v1", spaceId (groupId), accountId,
generation])`. The root admits its creator the same way: when the creator's
  generation is above 0, the root publication (space or group) carries the
  creator's admission notice. The server accepts an `admitted` notice only in
  the transaction of the root (for its creator) or of the account's first
  `add` there, so the notice form and the grant form cannot drift. For every
  account that is not reset-pending, an approval or admission notice of its
  current generation therefore exists, or its generation is 0. Admission
  notices raise nothing; they only record. An admission notice of generation 0 is
  malformed (generation 0 needs no notice).
- **History returns with approval.** The delivered key unwraps every earlier
  epoch key through the successors' `history` (§3.5), so an approved reset
  account regains full access to the space's history as well as its future.
  Keys are lost only for spaces and groups whose managers never approve.
  There is no cryptographic exclusion of history after a reset: the
  approval decides.
- **Members see it.** The approval row is the notice to members. Current
  recipients of the space (R11) and current members of the group (R17), and
  its managers, may read it; nobody else. A recipient's runtime watches the
  approvals of the spaces and groups it holds keys for, verifies each
  (signature by an author device active at its position, and the account's
  public roots showing generation `generation` before it, R10), and emits an
  `account-reset` encryption event `{account, space or group, generation,
approvedBy}` next to the unreadable-row reports (§4.3), so the application
  can show "<account>'s encryption was reset". An approval that fails
  verification is ignored and reported. It tells the space's members that this
  account is one of its recipients; that disclosure is the point (§8).
  Admission notices raise no event.
- **A reset manager.** A manager recipient that resets needs another manager
  to approve it; if it was the only manager, the space (group) stays without
  a usable manager until someone else gains the right and enrols (§8).

## 3. Groups and spaces

Let `D = Dₐ(creator)` for the group or space concerned.

### 3.1 Groups

**Records.** Let `G(column, recipient) = ctx{application: D, policy:
"jazz.e2ee.group.v1", scope: "group", identifier: groupId, table:
"__e2ee_groups", row: groupId, column, epoch, recipient}`, where `epoch` is
always the record's own `epochId` (the root's for the root and its marker,
the delivery's, notice's or successor's otherwise); `recipient` is empty
where not shown.

| Record            | Signed bytes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Root              | `frame[G(json(["root", accountId, deviceId, accountEpochId])), env(mechanism, verification)]`                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| Public keys       | `P = ctx{application: Dₛ, policy: "jazz.e2ee.group-public-key.v1", scope: "group", identifier: groupId, table: "__e2ee_group_public_keys", row: id, column: json([predecessorEpochId, proofEpochId, authorAccountId, authorDeviceId, authorEpochId]), epoch: epochId} ‖ frame[env(mechanism, publicKey), env(signingMechanism, signingPublicKey)]`. `keySignature` signs `P` with this epoch's group signing key, `predecessorSignature` with the proof epoch's group signing key (empty for the root epoch), `authorSignature` with the author device's key |
| Binding           | `ctx{policy: "jazz.e2ee.group-binding.v1", scope: "group", identifier: groupId, table: "__e2ee_group_bindings", row: groupId, column: json([scopeId, identifier]), epoch: root epochId, recipient: json([authorAccountId, authorDeviceId, authorEpochId])}` (unframed)                                                                                                                                                                                                                                                                                       |
| Rotation request  | `ctx{application: Dₛ, policy: "jazz.e2ee.rotation-request.v1", scope: "group", identifier: sourceGroupId, table: "__e2ee_rotation_requests", row: id, column: json([causeKind, causeId, sourceEpochId, authorAccountId, authorDeviceId, authorEpochId]), epoch: authorEpochId} ‖ frame[covers]`; `covers` is text: empty for `remove`, and the canonical ID list (§0) of the covered requests, possibly `[]`, for `rotation` and `coverage`. It is framed, not a context field, so its length is not bound by a field's limit                                |
| Membership        | `ctx{policy: "jazz.e2ee.group-membership.v1", scope: "group", identifier: groupId, table: "__e2ee_group_membership", row: id, column: json([operation, role, authorAccountId, authorDeviceId, authorEpochId]), epoch, recipient: json([memberKind, memberId])}` (unframed)                                                                                                                                                                                                                                                                                   |
| Successor         | `ctx{policy: "jazz.e2ee.group-successor.v1", scope: "group", identifier: groupId, table: "__e2ee_group_successors", row: id, column: "signature", epoch: new, recipient: json([authorAccountId, authorDeviceId, authorEpochId])} ‖ frame[predecessor, proofEpochId, commitment, verification, history, authorEnvelope]`                                                                                                                                                                                                                                      |
| Delivery          | `frame[G(json(["delivery", id, senderAccountId, senderDeviceId, senderEpochId]), json([recipientAccountId, recipientDeviceId, recipientEpochId])), envelope]`; the envelope is sealed with the same context                                                                                                                                                                                                                                                                                                                                                  |
| Child delivery    | `frame[G(json(["child-delivery", id, senderAccountId, senderDeviceId, senderEpochId]), json([recipientGroupId, recipientGroupEpochId])), envelope]`; the envelope seals the parent epoch key to the child's epoch public key with the same context                                                                                                                                                                                                                                                                                                           |
| Recovery delivery | `frame[ctx{policy: "jazz.e2ee.group-recovery.v1", scope: "group", identifier: groupId, table: "__e2ee_group_recovery_deliveries", row: id, column: json([senderAccountId, senderDeviceId, senderEpochId]), epoch, recipient: json([recipientAccountId, recoveryRootId, recipientEpochId])}, envelope]`                                                                                                                                                                                                                                                       |
| Repair            | `ctx{policy: "jazz.e2ee.group-repair.v1", scope: "group", identifier: groupId, table: "__e2ee_group_repairs", row: id, column: json([deliveryId, accountId, deviceId]), epoch, recipient: accountEpochId}` (unframed)                                                                                                                                                                                                                                                                                                                                        |

Where `application` is not shown it is `D`. In the root (and the space root,
§3.2), `env(mechanism, verification)` is applied literally: the root's
`mechanism` and `version` columns name the key wrapping mechanism of the
marker, and the signed field is the common envelope of that mechanism whose
payload is the whole `verification` column, which is itself already that
mechanism's envelope (`JE2E 01 ‖ len ‖ id ‖ u32be(version) ‖ verification`).
The successor `history` and
`authorEnvelope` use the successor context with columns `"history"` and
`"author-envelope"`; markers use `G("verification")` at the relevant epoch.

**Group ID.** A group's ID is a random UUID chosen at creation. It is opaque:
nothing derives it from, or reveals, the application row the group belongs to.
The link to the governing row is the group's `__e2ee_group_bindings` row,
signed by the creator and readable only by the group's managers and current
members (R16–R17); managers check its signature before acting on the group.
(The TypeScript implementation also let the creator choose the group ID, but
had no governing row at all.)

**Epoch key pairs.** For group epoch key `K`, the sealing key pair is HPKE
`DeriveKeyPair` of the input keying material
`BLAKE2b-256(key = K, input = UTF8("jazz.e2ee.group-kem-key.v1"))`; the
signing key pair's seed is
`BLAKE2b-256(key = K, input = UTF8("jazz.e2ee.group-sign-key.v1"))` as an
Ed25519 seed. Every holder of `K` can derive both. With each new epoch (root
or successor) the author publishes one `__e2ee_group_public_keys` row, in the
same transaction (R16), carrying one sealing and one signing key of the
mechanisms the configuration selects at that rotation (§0). The public-key
record and its epoch's root or successor are **one event**: the record's row
ID is the group ID for the root epoch and the successor's row ID otherwise,
so both readers order the same IDs. The record is signed three times over the
same bytes (§3.1 table): with the new epoch's signing key, with the signing
key of the **proof epoch** `proofEpochId` (an epoch of the chain at or before
`predecessorEpochId`; normally the predecessor itself; absent for the root),
and by the author device.

**Group epoch selection (public key chain).** For a group, the public-key
records alone decide which epoch is current, for every reader: outside
senders (parent and space managers, who never read the group's root,
successors or members), members and managers alike.

- The chain starts at the group's root record: the record whose ID is the
  group ID, with empty `predecessorEpochId` and `proofEpochId` (R16 admits it
  only with the group root, once). Its `keySignature` and `authorSignature`
  must verify, and its author device must have been active, with account
  epoch `authorEpochId`, at its position (public account replay, R10, as of
  that position, §2.4).
- From the current record (the tip), the next is the first record, in
  position order then ID, that is strictly later, names the tip's epoch as
  `predecessorEpochId`, names as `proofEpochId` the tip or an earlier epoch
  of the chain, has an epoch not used before in the chain, whose three
  signatures verify (`predecessorSignature` under the proof epoch's published
  signing key), and whose author device was active, with account epoch
  `authorEpochId`, at its position.
- Other records are ignored: records before the root, orphans, records of a
  losing branch, and second records of an epoch. A record with an
  unsupported mechanism leaves the chain unresolved from there (§7,
  unsupported capability); readers do not skip it.

The current epoch is the tip. An unapproved or removed device of a managing
account cannot extend the chain: it does not hold any chain epoch's key, or
is not active.

**Usable epochs.** Members and managers additionally need the epoch's key.
An epoch of the chain is **usable** when its successor row (same ID; for the
root, the group root) exists, is well formed, names the same predecessor,
proof epoch, epoch and author, and its signature verifies, and when the keys
derived from the epoch key equal the published ones. Otherwise it stays the
current epoch for everyone (so members and outside senders never disagree),
but it is **unusable**: members and managers report the group's current key
as unavailable for that epoch, and every manager sets `rotationRequired`.
The next rotation names the tip as `predecessorEpochId` and the latest usable
epoch it holds as `proofEpochId`, signs with that epoch's key and wraps that
key in its `history`. This is how the group gets past a record whose
published key nobody else controls (a client defect or a manager acting in
bad faith): outside senders seal to the bad key only until the next honest
rotation. Managers use the predecessor as proof epoch whenever it is usable;
skipping a usable epoch only makes that epoch unreachable through `history`
from later keys.

**Key links.** A group's event chain (the public key chain) and its key links
differ only where an epoch was skipped. Each usable successor's `history`
wraps the key of its proof epoch, so key links run from each usable epoch to
its proof epoch, back to the root. Acquiring a group key (§3.5 step 6) starts
from the opened key of a usable epoch, checks that epoch's marker
(`G("verification")` at that epoch, from its root or successor row), unwraps
`history` to the proof epoch, checks the proof epoch's marker, and continues
until the root. The chain order and every author check (as of each record's
position, §2.4) come from the public-key records; the skipped, unusable
epochs stay in the chain as events but have no key: cells and deliveries of
those epochs report `key-unavailable`, and nobody asks for their key again.
For example, with chain `G0 → G1 → G2`, `G1` unusable and `G2` naming
`proofEpochId = G0`: a member opening `K2` obtains `K0` through `G2`'s
`history`, holds `K2` and `K0`, and knows `G1` as an unusable event; a cold
device and a restarted one (from its stored lineage, §6.2) reach the same
state. The "full history" of a group therefore means every usable epoch.
The author columns make the publishing account of each epoch
visible to every signed-in session (§3.5, §8). (The TypeScript implementation
had no group key pairs.)

**Creation.** Two publications: the root with its binding and public-key
record (the creator is the sole initial direct member), then `initial-delivery` to every
member device. If the second fails, the error carries `confirmedRootId`.

**Replay** (by group managers, who read every record). Events sort by
position, then root < successor < membership, then ID by code unit (the same
rule as spaces; the TypeScript implementation used a locale comparison here).
Membership records apply only after the root position and only for the current
epoch; any active device's correctly signed record applies (authority is Jazz
policy, R16). An account add binds the account's epoch at that position. A
group add binds the child's current epoch from its public key chain. Roles
are tracked separately from key replay, from accepted rows (R25): a `manager`
`add` of an account already present as a member makes it a manager recipient
without changing its membership. The
members of a group are its direct accounts and its child groups; a parent
never reads or replays a child's own members. Removing an account or a child
sets `rotationRequired`; no members left **seals** the group permanently. A
direct member whose account epoch changed makes the group stale. A child's
rotation, and a removal from a child, reach the parent through rotation
requests (below), not through replay.

**Commitment.** A successor does not carry its member list. It carries
`commitment = BLAKE2b-256(key = salt, input =
UTF8("jazz.e2ee.recipient-commitment.v1") ‖ 0x00 ‖ frame[membership,
revision])`, where `salt` is 32 fresh random bytes and `membership` and
`revision` are the canonical encodings of §0. The salt, membership and
revision are stored in the manager-only `__e2ee_group_successor_recipients`
row with the successor's ID, inserted in the same transaction. A member
verifies the successor's signature, which covers the commitment; a manager
recomputes the commitment from the recipients row, and a missing or
mismatching row sets `rotationRequired` (chain rule, below). The hash is keyed with a
secret salt because the inputs have little entropy: account IDs and their
account epochs are public (R10), so an unkeyed hash would let any member
confirm a guessed member list, and keying with the epoch key would not hide it
from members, who hold that key. The salt makes the commitment hiding toward
members while keeping it binding for managers, who read the salt.

**Chain rule.** One rule decides the current epoch, for every reader alike.
For a group it is the public key chain above. For a space, starting from the
root, the next epoch is the first successor, in position order then ID, that:
names the current epoch as `predecessor`; is strictly later than the current
epoch's position; has an epoch not used before; is well formed; has an author
device that was active, with account epoch `authorEpochId`, at its position
(account replay as of that position, §2.4); whose signature verifies; and
whose author passes the pending-author rule below. Nothing else decides
acceptance: in particular, a manager does not reject a successor
because its recipients row is missing or differs from its own replay, or
because no rotation was needed. A manager that finds the recipients row
missing, not matching the commitment, or not equal to the membership and
revision its own replay expects, or a rotation that was not needed, keeps
the successor as current and sets `rotationRequired` on top of it, so the
next manager rotates again from it. Members and managers therefore never
disagree about which epoch is current, and one bad successor costs one extra
rotation instead of splitting the group.

**Pending-author rule.** A governance record of a space or group (a space
successor, a grant or membership record, a delivery, a notice or approval, a
key request or its answer) counts for a client only if its author passes, in
notice form: the author's generation as of the record's position (§2.4) is
0, or an approval or admission notice of that space (group) for that account
and generation is accepted at or before the record's position. Members and
managers can all read the notices (R11, R12, R17), so every client applies
the same test; the grant-based admitted generation (§2.11) is the server's
test and the source of `pendingResets()`. Space successors are subject to
it, so a space's chain stays one rule for every reader. Group public-key
records are not: outside senders cannot read a group's notices, and a
pending account's new devices hold no key of the group's chain, so they
cannot produce a `predecessorSignature`; the server's refusal (§2.11) is the
guard there.

**Rotation** is by a group manager holding the current group key. `membership`
is the member map of the direct member accounts' current epochs; `revision`
lists `"group-member:" ‖ id` for each accepted membership record and one
`"group:" ‖ json([childId, childEpochId])` per child group, encoded as a
revision. Concurrent successors of one predecessor resolve by the chain rule;
read-set validation (§0 item 5) normally refuses the second at commit.
(The TypeScript implementation required a direct-member author, signed the
member list itself, listed every visited membership record in the revision
and rejected all successors sharing a position and predecessor; successors it
wrote do not verify under these rules.)

**Delivery.** A manager seals the group epoch key to every active device of
every direct member account (as before) and to each child group's current
public key (a child delivery), unless the delivery floor below blocks
that child. Members of a child obtain the parent key by opening the child
delivery with the child's box key, which they derive from the child epoch key
they hold.

**Child rotation.** When a child group rotates, the child's managers re-wrap
every key the child holds (parent keys, and space keys delivered to the child)
for the child's new epoch, writing new child or space group deliveries
addressed to the child (R21). A member removed from the child does not
receive the child's new key and so cannot open any later re-wrap or any key
delivered to the child afterwards.

**Rotation requests.** A member removed from group `C` still holds `C`'s
current key and, through it, the current keys of every group and space `C`
is nested in, along every path. So does a device removed from a direct
member account of `C`. Every ancestor must stop using keys the removed
member holds, and no ancestor may seal a new key to one.

- The transaction that removes a member from `C` (a manager's removal, or a
  member's own self-removal, R26) inserts a request with
  `sourceGroupId = C`, `causeKind` `remove` and no `sourceEpochId` (R23).
- Every successor of `C` (whatever caused it: a removal, a stale member
  account, requests of its descendants) is inserted together with a request
  with `causeKind` `rotation`, `sourceEpochId` = the new epoch, and `covers`
  = the IDs of the descendants' requests (readable to `C`'s managers) that
  this epoch is clear of (below). A parent's rotation therefore produces the
  request its own parents need, naming what it covers.
- When `C`'s managers consume a descendant request **without** rotating
  (because `C`'s current epoch never reached a key not clear of it), the same
  maintenance run inserts a request with `causeKind` `coverage`,
  `sourceEpochId` = `C`'s current epoch, `causeId` = that epoch's successor
  (or root) ID and `covers` = those requests. A `coverage` request asks
  nothing of ancestors (it is never outstanding); it only lets them see that
  `C`'s existing epoch is clear.
- `covers` lists only requests that no earlier `rotation` or `coverage`
  request of `C` already covers: an earlier epoch's coverage carries over
  (clear keys, below), so each request is listed once per group and lists
  stay proportional to new removals.
- `covers` is the writing manager's statement, under the delivery floor,
  that its epoch was and will be delivered only to keys clear of the covered
  requests. The deliveries it promises may follow in later publications, and
  ancestors cannot check it (they cannot read `C`'s deliveries); like any
  record by a manager it rests on Jazz authorising that manager (§8).
- A request of `C` is readable by the managers of every group and the key
  managers of every space in which `C` is nested (R23), together with its
  **paths**: for each such ancestor `A`, the direct children or group
  recipients of `A` through which `C` is reached. The server computes paths
  at read time from the current-edge projection (a bounded gather from `A`
  down to `C`), without storing a row per request; they are not signed, and
  like every authorisation decision they rest on the server (§8). The writer
  needs no knowledge of the ancestors.

**Clear keys.** For an ancestor `A`, a request `q` of a descendant `C` and a
direct child or group recipient `G` of `A` on a path to `C`, a key of `G`'s
epoch `e` (in `G`'s public key chain) is **clear of `q`** iff:

- `G = C`: for `causeKind` `remove`, `e`'s record is strictly later than `q`;
  for `causeKind` `rotation`, `e` is `q.sourceEpochId` or a later epoch of
  `G`'s chain (a `coverage` request is never itself an obligation: it is only
  a proof annotation for the rule below, and is never outstanding or
  retired);
- `G ≠ C`: some `rotation` or `coverage` request of `G` whose
  `sourceEpochId` is `e` or an earlier epoch of `G`'s chain covers `q`,
  directly or through the `covers` lists of the requests it covers (each
  readable to `A`'s managers, since they are all nested in `A`).

Clearness only grows as `G`'s chain moves forward. `C`'s own managers decide
what an epoch of `C` covers by the same rules, one level down: a descendant
request is covered by an epoch that is delivered only to keys clear of it.

**Delivery floor.** `A`'s managers never deliver any `A` epoch to a key of
`G` that is not clear of every request reachable through `G` and accepted
before the delivery, whether or not that request has been consumed. `G`'s
delivery stays pending until `G`'s current key is clear. The floor is
permanent: consuming a request ends `A`'s obligation to rotate for it, not
the floor.

**Consumption.** A request `q` is **consumed** for `A` when no delivery of
`A`'s current epoch, through any path to `q`'s source, is addressed to a key
that is not clear of `q`. Until then it is **outstanding** and sets
`rotationRequired`; `A` rotates and, by the floor, withholds the new epoch
from paths not yet clear, which consumes `q`. A request accepted after `A`'s
current epoch was last delivered through its paths, to clear keys only, is
consumed at once with no rotation. An intermediate child's existing key may
be clear but not yet shown to be (its managers have not yet written the
`coverage` or `rotation` request that covers `q`). `A` cannot tell that from
a key that is not clear, so by default it does not wait: it rotates at once
and withholds from that child until the child shows its key clear. A
configurable interval (default none) lets `A` wait for the child's coverage
first, trading a possible extra rotation against the time a removed member
can still read `A`'s current epoch (§8). Replay recomputes consumption from
the rows, so nothing is stored.

**Requests are retained.** The server keeps returning every request readable
to an ancestor's managers (R23), with its paths computed at read time from
the current edges; it never hides or prunes a request on the strength of
coverage or public-key records, which only clients authenticate. Each
client decides consumption and keeps its own authenticated floor: a
per-ancestor cursor of requests it has seen consumed with every path child
clear, valid only for the set of paths it was computed over. When a new path
to a request's source appears (a new grant or child of `A`, or an edge change
further down that makes `C` reachable through another child), the cursor no
longer applies to that request: the client evaluates it again through the
new path, and an uncleared new path re-opens the floor, and, if `A`'s current
epoch already reached a key on it, the obligation. For example: `S` grants
`C`; `P` also contains `C` but `S` does not grant `P` yet. `C` removes Bob and
rotates to `C1`; `S` rotates to `S1`, delivers only to `C1`, and its cursor
passes the requests. `S` later grants `P` at `P0`, which Bob holds through
`C0`. The new path `S → P → C` invalidates the cursor for `C`'s requests;
`P0` is not clear of them, so the floor withholds `S1` from `P0` until `P`
shows a covering request (and, had `S1` already reached `P0`, `S` would
rotate again). A cold device reads the retained requests once and builds its
cursor; a client may send its cursor so the server returns only later
requests, but the server never decides retirement itself. A manager
evaluates paths, clear keys, floor and consumption on the ledger read of its
publication (§0 item 3); read-set validation (§0 item 5) refuses the
publication if a new request, path or child key arrives before commit.

This covers the cases:

- **Ancestor before child.** `A` rotates on `C`'s `remove` request and
  withholds the new epoch from `C`; `q` is consumed. When `C` rotates, its
  `rotation` request is consumed at once (`A`'s current epoch was never
  delivered to a key of `C` not clear of it), and `A` delivers its current
  epoch to `C`'s new key, which the removed member lacks. No second rotation
  of `A`; if `A` is a group, it writes a `coverage` request for its existing
  epoch, covering `C`'s requests.
- **Parent before child, three levels.** `C` in `P` in `S`. `C` removes Bob
  and writes `q`; `P` rotates to `P1`, withholds it from `C` and writes `rP`
  covering `q`; `S` may deliver `S1` to `P1`. `C` then rotates to `C1` and
  writes `rC`. `P1` never reached `C0`, so `P` consumes `rC` without
  rotating, writes a `coverage` request for `P1` covering `rC`, and delivers
  `P1` to `C1`. `S` sees `rC` with path `P`: before `P`'s coverage arrives,
  `P1` is not shown clear of `rC`. If `S` had not yet delivered to `P1`, its
  delivery waits for it; if it had, `rC` is outstanding for `S`. By default
  (no coverage wait) `S` rotates to `S2` at once and withholds it from `P`;
  when `P`'s coverage arrives, `P1` is clear and `S` delivers `S2` to it. With
  a configured wait, `S` first waits up to the interval: if `P`'s coverage
  arrives in time, `rC` is consumed with no rotation and `S` keeps its
  delivery; otherwise `S` rotates as above. Either way delivery never stalls
  while `P`'s managers are online.
- **Several levels.** With `C` in `P` in space `S`: `S` reads `C`'s requests
  with path `P`. `P`'s key is clear of them only once a `P` rotation request
  covers them, so `S` withholds from `P` until then.
- **Diamond, parent offline.** `C` is a child of `P`, and `S` grants both `C`
  and `P`. `C` removes Bob and rotates to `C1`; `P`'s manager is offline, so
  `P` is still at `P0`, which Bob can open through `C0`. `S` reads `C`'s
  requests with paths `C` and `P`. Through `C`, `C1` is clear; through `P`,
  `P0` is not (no `P` request covers them). `S` rotates to `S1`, delivers to
  `C1` and withholds from `P`. When `P` rotates and its request covers
  `C`'s, `S` delivers `S1` to `P1`.
- **Rotate, withhold, then deliver.** After `A` has consumed `q` by
  withholding, a later maintenance run still obeys the floor for `q`: it
  delivers to `C` only once `C`'s key is clear.
- **Concurrent removals.** Several requests of `C` are outstanding at once;
  a key must be clear of each, and one rotation of `A` consumes all of them.
- **Device removal.** A device removed from a direct member account makes
  `C` stale; `C`'s rotation writes a `rotation` request. Between the account
  rotation and `C`'s rotation there is no request yet, so `A` may still seal
  to `C`'s previous key; the removed device keeps what it can open until
  then, as for any epoch before a rotation (§8).

Jazz permissions stop row delivery to the removed member immediately. Keys it
already holds are not revoked and stay usable for content of their epochs.
A request's signature proves its author; a forged or redundant request can
only cause an extra rotation or a delayed delivery. Removing a child group
`C` from a parent `P` is an ordinary removal in `P`: it writes `P`'s own
`remove` request for `P`'s ancestors, and needs no request authored by
`C`.
Until `C` rotates, `C`'s remaining members cannot read the new epochs of
ancestors that rotated first, since those are withheld from all of `C` (§8).
Ancestors may coalesce requests: a manager may wait up to a configured
interval (default none) before rotating, and one rotation consumes every
request it satisfies. Requests name their source group, so `A`'s managers can
see which child causes rotations, and remove it if it churns (§8).

**Repairs.** A valid repair (later than the delivery, by a member device of
matching account epoch) voids that delivery so a maintainer redelivers.
Spaces have the same repair (`__e2ee_space_repairs`, §3.4).

**Unresolved history.** A record needing a capability this client lacks
(unsupported ledger version or signing mechanism) leaves the group
**uncertain** for its own managers and members; operations that need the
uncertain state fail with `GroupHistoricalUncertaintyError`. Because parents
and spaces never replay a child's history, a child's uncertainty no longer
blocks its parents or spaces. Deliveries made while uncertain are not
canonical. (The TypeScript implementation propagated a child's uncertainty to
its parents and to spaces, reporting `blockingGroupId`.)

**Nesting limits.** The server refuses a nesting row that would create a
cycle or a chain of more than 8 nested groups (R20). Clients also stop at 8
levels when unwrapping through groups and ignore any cycle they meet.

### 3.2 Space records

The space root ID is derived from the address:
`b = SHA-256(UTF8(json(["jazz.e2ee.space-id.v1", scopeId, identifier])))[0..16]`,
`b[6] = (b[6] & 0x0f) | 0x80`, `b[8] = (b[8] & 0x3f) | 0x80`, formatted as a
lowercase UUID. Only a root with this ID names the space; any other root row
with the same address is ignored.

Let `X(column, recipient, epoch) = ctx{application: D, policy:
"jazz.e2ee.space.v1", scope: scopeId, identifier, table: "__e2ee_spaces",
row: spaceId, column, epoch, recipient}` (all four coordinates must be UUIDs).

| Record            | Signed bytes                                                                                                                                                                                        |
| ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Root              | `frame[X(json(["root", creatorAccountId, deviceId, accountEpochId, initialGrantId])), env(mechanism, verification), authorEnvelope]`                                                                |
| Grant             | `X(json(["grant", id, authorAccountId, authorDeviceId, authorEpochId, operation, role]), json([recipientKind, recipientId, recipientEpochId]), keyEpoch)` (unframed); requires `epochId = keyEpoch` |
| Successor         | `X(json(["successor", id, "signature"]), json([authorAccountId, authorDeviceId, authorEpochId]), newEpoch) ‖ frame[predecessor, commitment, verification, history, authorEnvelope]`                 |
| Delivery          | `frame[X(json(["delivery", id, senderAccountId, senderDeviceId, senderEpochId]), json([recipientAccountId, recipientDeviceId, recipientEpochId]), epoch), envelope]`                                |
| Group delivery    | `frame[X(json(["group-delivery", id, senderAccountId, senderDeviceId, senderEpochId]), json([recipientGroupId, recipientGroupEpochId]), epoch), envelope]`                                          |
| Recovery delivery | `frame[X(json(["recovery-delivery", id, senderAccountId, senderDeviceId, senderEpochId]), json([recipientAccountId, recoveryRootId, recipientEpochId]), epoch), envelope]`                          |
| Repair            | `X(json(["repair", id, deliveryKind, deliveryId, accountId, deviceId]), accountEpochId, epoch)` (unframed); a recipient's claim that the named delivery to it does not open                         |

The root's own contexts (its signed bytes, its marker and its author
envelope) use the root's `epochId` as `epoch`; every other record uses its
own `epochId` (`newEpoch` for a successor). Key material: root
`verification = wrap(K₀, X("verification"), Z32)` and
`authorEnvelope = seal(device.pub, X("author", deviceId), K₀)`. A successor
uses `X(json(["successor", id, role]), …, newEpoch)` with roles
`"author-envelope"` (recipient = author device) and `"history"` (empty
recipient):
`authorEnvelope = seal(author.pub, …, K')`, `history = wrap(K', …, K)`,
`verification = wrap(K', X("verification", "", newEpoch), Z32)`. A delivery's
envelope is `seal(device.pub, <its context>, K)`; a group delivery's is sealed
to the group's epoch public key; a recovery delivery's to the root's public
key. The successor `commitment` is computed exactly as for groups (§3.1), over
the space's `membership` and `revision`, with the salt and lists stored in
`__e2ee_space_successor_recipients`. (The TypeScript implementation signed
`revision` and `membership` directly; its successors do not verify under this
format.)

### 3.3 Creation

A space is created atomically with its first rows: the root, its grants and
the application rows (including the scope row, when new) are staged in one
mixed ledger transaction. Preconditions: an active author device; recipients
default to the author's account, must be distinct and nonempty, and always
include the author's account (a manager, §3.8). When the space table declares
`recipients` (§1.5), the initial recipients are the author's account plus the
members named by the membership rows staged in the same transaction; the
recipient sync (§3.8) reconciles the rest. Generate
`epochId` and `K₀`. Each recipient gets an `add` grant on the root epoch;
the first is the creator's own `manager` enrolment, whose ID
`initialGrantId` is derived like every self-enrolment grant (§3.8), with
`n = 0`; the root names it and replay refuses a root whose
`initialGrantId` differs from that derivation. The other initial grants
have random IDs. `recipientEpochId` is the
account's current epoch, or for a group the current epoch of its public key
chain (§3.1). No delivery is written at
creation: the root `authorEnvelope` is the creator's own copy, and other
initial recipients receive deliveries from later maintenance. The creator's
grant has role `manager`; the others have role `member`.

**When a space is created.** A space exists once its root is accepted.
Creation needs the `manageKeys` right (R13), and happens in these cases:

- **First encrypted write.** Inserting or updating an encrypted row of a
  space with no root, by a session that manages it, creates the space in the
  same transaction (§5.4), whether or not the row is the governing row.
- **Governing row by a manager.** After a session inserts a governing row
  (a row of a table that some `encryption` entry names as `scope`), its
  runtime asks for permission advice on the accepted row and, if the session
  manages it, creates the space in a follow-up publication, whether or not the
  scope table itself has encrypted columns (in the §1.5 example, `rooms` is
  plain and `messages` encrypted). Creation does not ride in the
  application's own transaction, so an E2EE refusal never fails the
  application's write.
- **Discovery by managers.** Every online manager's runtime discovers
  governing rows readable to it whose space has no root (a key manager reads
  every space record, R12, so a missing root is visible to it), confirms
  management through permission advice, and creates the space. This covers
  governing rows inserted by sessions that do not manage them (for example
  when `manageKeys` goes through a workspace), and rows that existed before
  encryption was declared. Concurrent creators produce the same derived root
  ID; the authority accepts one and the others re-read.
- **Non-managers.** A session that does not manage the space never creates
  it. Its encrypted write to a root-less space is refused with
  `space-pending` ("no key manager has created this space yet"); its runtime
  watches for the root and the application may retry once it appears. A read
  of a root-less space returns no encrypted rows (none can exist, since
  creation is atomic with the first encrypted rows) and, for a declared member,
  reports the space `space-pending` like `key-pending` (§4.3).
- **Schema conversions** create missing spaces the same way, through the
  sweep's managers ([migrations](E2EE_MIGRATIONS.md) §3.5).

Replay accepts the root iff its ID is the derived ID, the creator had an
account root strictly before it, the device was active and the account epoch
equal to `accountEpochId` there, and the signature verifies. The initial grant
must be at the **same position** as the root (atomic coverage); other grants at
that position count only if they are `add`s by the root author, device and
epoch on the root epoch. The creator is a key manager (creation requires the
R13 right), so it is always among the initial recipients, with a
self-authored `add` (§3.8); in manual mode the application may name other
initial recipients but cannot exclude the creator.

Offline, the root and initial grant must share one own pending transaction.

### 3.4 Recipient changes, rotation and delivery

**Staging a change** requires an active author device; authority is Jazz
policy (R13). A sealed space refuses. A manager may also remove another
account's `manager` role (a `remove` grant of role `manager`), in declared
mode too; this is how managers drop a former manager that recipient
confirmation cannot decide (§3.8). When the space table declares
`recipients`, explicit changes by the application are refused with
`recipients-declared` ("recipients of `rooms` follow the `members` relation;
add or remove membership rows instead"); only the recipient sync (§3.8)
stages them. `remove` requires the recipient to be
present and binds its replayed epoch. Adding an account binds its current
account epoch; adding a group binds the current epoch of its public key chain (§3.1)
(no right on the group's row and no reading of its members, R20). The grant's
`epochId` is the space's current key epoch. A change can be staged alone or in
an ordinary transaction with application rows (for example the application
membership row), so both commit together.

**Replay of grants** (by key managers). Events sort by position, then
successors before grants, then ID by code unit. A grant applies iff its epoch
is the current key epoch, its author device was active with matching account
epoch at its position, its signature verifies, and: `remove` names a present
recipient with the same bound epoch (then `rotationRequired`); `add` of an
account matches the account's epoch at that position (an account enrolled
later is rejected); `add` of a group names a group with a public-key record
of that epoch. Re-adding with a different epoch also requires rotation. An
`add` of a recipient already present with the same bound epoch, and a
`remove` of an absent recipient, are ignored (not applied, not errors). Roles
are tracked separately from key replay, from accepted rows (R25); a
`member`-role `remove` of a recipient that is still a manager recipient
removes only the member role, and the recipient stays present. A
direct recipient whose account epoch has changed makes the space **stale**; a
group recipient's rotation does not by itself, but every outstanding rotation
request of a group recipient (§3.1) sets `rotationRequired`. Accepted grants accumulate into the space
revision as `"space-grant:" ‖ id`, plus one
`"group:" ‖ json([groupId, boundEpochId])` per group recipient (the TypeScript
implementation included the group's sealed flag and revision, which a space
manager can no longer read).

**Sealing.** When no direct recipient and no group recipient remains, the
space is sealed permanently; later grants are ignored.

**Maintenance** runs as accepted publications: rotate if `rotationRequired` or
stale, else deliver; then, after a rotation, deliver. A space manager never
maintains a recipient group.

- **Rotation** by a key manager (§1.4) holding the current key, after it has
  confirmed every manager recipient (§3.8). New epoch and
  key `K'`; `membership` = member map of the direct recipients' current
  account epochs; `revision` = the replay's revision; both behind the
  commitment. Which successor is current is decided by the chain rule of
  §3.1, for managers and recipients alike; a manager that finds the
  recipients row missing or different from its expected bytes, or a rotation
  that was not needed, sets `rotationRequired` on top of it. (The
  TypeScript implementation let any effective member rotate.)
- **Delivery.** For each direct recipient whose current account epoch equals
  the bound one (otherwise "needs maintenance"), seal the epoch key to every
  active device lacking an accepted, unrepaired delivery and to every active
  recovery root lacking one. For each group recipient, seal it once to the
  group's current public key (the end of its public key chain, §3.1), unless
  the delivery floor of §3.1 blocks it. Devices or roots with an
  unsupported mechanism are skipped; they remain recipients without a usable
  delivery.
- **Repairs.** A recipient device whose delivery is correctly signed but does
  not open inserts a space repair (R19) naming it; a valid repair (later than
  the delivery, by a device of the named account at matching account epoch)
  voids that delivery, so the next maintenance redelivers.
- **Manager without the key.** A manager recipient that lost the key (for
  example, every manager moved to a new device without recovery) cannot
  rotate or deliver, because a successor needs the old key for its `history`
  link. Its runtime then writes a **key request** (R27) for the current epoch
  and one of its active devices, signed by that device over
  `ctx{application: D, policy: "jazz.e2ee.key-request.v1", scope: scopeKind,
identifier: scopeId, table: "__e2ee_key_requests", row: id, column:
json([accountId, deviceId]), epoch: epochId, recipient: accountEpochId}`.
  Members' runtimes watch the key requests of the spaces and groups they
  hold keys for (H6). A member holding the requested epoch's key answers with
  a delivery to the named device if the request verifies (device active with
  matching account epoch, signature valid, epoch still current) and the
  requesting account is not reset-pending: its generation is 0, or an
  approval or admission notice of its current generation exists in the space
  (group), which members can read (§2.11). A member never approves a reset;
  it leaves such a request to managers. The answer's row ID is the version 8
  ID derived from `json(["jazz.e2ee.key-request-answer.v1", requestId])` (§0),
  so the authority accepts one answer per request; a member whose answer is
  refused stops (a refusal looks the same whether another member answered or
  policy refused). Members answer after a short random delay, so usually only
  one tries.
  **Lifetime.** A request is open while its epoch is current and no later key
  request of the same device for the same space (group) and epoch exists.
  An accepted answer does not close it by itself: the requesting device
  verifies the answer (§3.5); if it does not verify or open (malformed,
  signed by an inactive device, or a validly signed delivery of a key whose
  marker fails), the device writes a repair naming it (R19) and a new key
  request, which supersedes the old one and gets its own single answer. When
  the key verifies, the device stops requesting and the request simply
  stays answered. A member that answers requests with keys that fail is
  named by its answers' sender columns; the remedy is for a manager to
  remove it from the space (group), which needs no key. A request is a
  status visible to members only, never a grant. The SDK
  also exposes it (`db.encryption.keyRequests()`), so an application may let
  a person answer it explicitly.
- **Reset re-binding.** An `add` of a present recipient at a different
  account epoch normally requires rotation; when a valid reset approval of
  that account at its new generation is at the same position (§2.11), it
  re-binds the recipient without rotation.
- **Self-delivery.** Any device of a current recipient account holding a key
  may deliver it to the account's other active devices (R22); this needs no
  management right. It is refused while the account is reset-pending in that
  space (group): an old device of a reset account (possibly the victim's,
  possibly lost) never delivers to the new generation's devices (§2.11).
- A pending rotation never blocks reads or writes. Until a manager rotates,
  every client keeps reading and writing with the epoch keys it holds, which
  may be stale (§5.1); content written meanwhile is readable by a removed
  recipient that still holds that epoch's key.
- Rotation does not re-encrypt existing content: old cells stay under their
  epochs. Re-encrypting old content on rotation, for example as an opt-in for
  high-churn or high-sensitivity spaces and groups, is out of scope and not
  specified here.
- Key versions accumulate with churn: every removal adds an epoch. Reads try
  the held key of each cell's epoch, and equality queries compute one token
  per held epoch (§4.5); each rotation costs one delivery per remaining
  direct recipient device, or one per group recipient, which is why nesting
  groups reduces it. The cost is to be measured at the core hooks.
- Removal does not require the administrator to hold the key. Rotation and
  delivery are separate operations; a later content write never completes
  them. In manual mode the application triggers them; with declared
  `recipients` the recipient sync (§3.8) runs them.
- Removal withholds future keys only. A removed recipient keeps every key it
  already holds, and those keys stay usable for content of their epochs;
  rotation ensures content written under the new epoch uses a key it never
  receives, and Jazz permissions stop delivering it rows.

### 3.5 Verifying history when acquiring a key

Space reads are self-only (R11–R12), so verification has two levels.
Authorisation (who may grant, rotate or deliver) is decided by Jazz on the
server; a client verifies authorship and signatures, not authorisation.

**Recipient verification** (every client acquiring a key). The client reads,
at one cut, the space root, the key-version chain, the grants naming its
account or its groups, the deliveries addressed to it or its groups, and the
public account histories of the authors and senders involved. Then:

1. **Not a member** (`SpaceKeyNotMember`, definitive) if no path makes this
   account a current recipient (per role, R11): no accepted grant names it or
   a group it belongs to, or on every path some step (space grant, then each
   group membership down to the account) has both roles in a `remove` state,
   or the space is sealed. A reset-pending account (§2.11) is a current
   recipient waiting for approval (`key-pending`), not a non-member. A removed recipient sees its own `remove` grant (R11).
2. This device must be active, not removed, with a supported mechanism and the
   registered public key; a recovery credential must match a current recovery
   root.
3. The client authenticates the root (derived root ID, author device active
   with matching account epoch at its position, valid signature) and finds
   the current epoch by the chain rule (§3.1), the same rule managers use. A
   recipient cannot and does not open the commitment.
4. Candidate sources, in order: the root `authorEnvelope` (creator device, no
   successor yet), the current successor's `authorEnvelope` (if this device
   wrote it), deliveries of the current epoch to this device or recovery root
   (from a manager, a self-delivery from its own account, or a member
   answering this device's key request, R27), and group
   deliveries of the current epoch to a group it belongs to. A delivery is
   accepted iff it is strictly after the root, names the current epoch, names
   this account at its current account epoch in `recipientEpochId` (or the
   group at an epoch whose key this client holds), its sender device was
   active with matching `senderEpochId` at its position, and its signature
   verifies. Group deliveries and group recovery deliveries carry and sign
   `senderEpochId` and `recipientEpochId` too (§3.1), and are checked the same
   way. Whether the
   sender was entitled to deliver is Jazz's decision, not re-checked.
5. For a group delivery, the client first obtains that group's epoch key the
   same way (its own group deliveries, or child deliveries through a nested
   group, at most 8 levels, ignoring cycles), derives the group's box key and opens the
   delivery.
6. The opened key is authenticated by its marker, then each `history` is
   unwrapped back to the root, checking each predecessor against the chain.
   Earlier epoch keys are thereby obtained too. For a group, `history` leads
   to each epoch's proof epoch (key links, §3.1): skipped, unusable epochs
   are kept as events without a key.
7. If nothing is accepted: **delivery pending** (`SpaceKeyDeliveryPending`)
   when a current-recipient path exists and no delivery reaches this device
   yet; otherwise **key unavailable**.

**Key-manager verification.** A key manager reads every record of the space,
including the recipients rows, and replays the full recipient history (§3.3–
§3.4) before it rotates, delivers or adds a recipient, so that the
commitment, membership, revision and recipient set it writes are exactly what
replay expects. Group managers do the same for their group.

(The TypeScript implementation had every acquiring client read and replay the
full space evidence, including other recipients' grants and account
histories, and the group closure.)

**What remains visible to recipients.** The root reveals its creator and the
initial grant ID; each successor reveals its author, epoch and timing. The
member list is behind the commitment. Group public-key records, readable by
every signed-in session, reveal that a group ID exists, when it rotates, and
which account published each epoch (the author columns the public key chain
needs, and the row's `$createdBy`); that account is necessarily a manager of
the group. They do not reveal the governing row or the members. A limited
public projection that hides the author from outsiders, while still proving
the chain, is possible later privacy work.

Groups and spaces treat questionable deliveries alike: a delivery at or
before the root position, or one whose sender uses an unsupported signing
mechanism, is skipped (not accepted, not an error). (The TypeScript
implementation threw for an early group delivery and for an unsupported sender
mechanism on a space delivery.)

### 3.6 Recovery and new devices

Key managers seal each space or group epoch key to every active recovery root
of every direct recipient account. Restoring with a recovery secret or with
exported material:

1. The restored device obtains the account key (§2.9) and opens the space and
   group recovery deliveries addressed to its account, authenticating each key
   as in §3.5.
2. It keeps those keys as held keys and self-delivers them to its own active
   devices, itself included (R22). No key manager is involved.

Recovery cannot restore access for a removed recipient. A member reached only
through a group recovers its group keys this way and then reaches spaces
through group deliveries.

After a new device is approved (§2.5), any of the account's devices holding
keys self-delivers the current epoch key of each space and group it holds to
the new device, in publications separate from the approval. Approval is
complete without them; a missing one can be delivered later by any device of
the account, or by a manager's maintenance.

(The TypeScript implementation had the restoring client run space maintenance
itself, and relied on managers' maintenance to deliver to a newly approved
device.)

### 3.7 Offline (provisional) governance

Provisional operations replay over a provisional snapshot (§0). Kept
predicates: the author's account history (active device, not removed, keys and
mechanisms matching) and the evidence the author may read for any space
created, changed or whose key is acquired. Provisional rotation writes only
the successor and its recipients row. Provisional space deliveries are not
claimed before accepted maintenance. On reconnection, Jazz permissions decide
acceptance.

### 3.8 Managers as recipients, and recipient sync

**Managers are recipients.** Every account that passes a space's (or group's)
`manageKeys` check is a recipient of the space (or a member of the group), in
manual and declared mode alike. When a manager's client is online and finds
(through Jazz's online permission advice for the `manageKeys` operation on the
governing row) that its account manages a space and is not a manager
recipient, it stages one `add` grant of role `manager` naming its own
account, authored by itself, even when it is already a recipient as a
member. Such an account is a **manager recipient** (§1.1, R25). Any manager
holding the current key then delivers to it as to any recipient; if no
manager holds the key, any recipient holding it may answer the manager's
key request (R27, §3.4). The creator is a
manager recipient from creation (§3.3). Groups do the same with `manager`
membership records; a group's creator is its first member.
Consequences:

- A manager always holds, or is about to receive, the current key, so it can
  deliver to a new member at the current epoch. Adding a member never needs a
  rotation, and no manager ever rotates just to be able to add someone.
- Managers can read the content they manage. Who may manage keys is therefore
  also who may read; an application backend acting as manager would hold the
  keys and defeat end-to-end encryption.
- A manager recipient stays a recipient while it manages, even if the
  membership relation stops naming it.

**Declared members.** When a space table declares `recipients` (§1.5), the
**declared members** of a space are the accounts and groups named by the
accepted rows of its membership relation. The **desired recipients** are the
declared members plus the manager recipients. The recipient sync keeps the
replayed recipients (§3.4) equal to the desired ones. A group table with
`recipients` is synced the same way, with membership records (§3.1) in place
of grants and the group's managers running it.

**Who runs it.** Every online client of a key manager runs the sync for the
spaces and groups it manages; it is part of the client runtime, not
application code, and needs no right beyond R13 (or R16), R24 and the read
requirement of R15. A client learns the spaces it may manage from the space
roots readable to it (R11–R12) for scope tables that declare `recipients`,
from governing rows readable to it that have no space root yet (it then
creates the space, §3.3), and the groups from the group roots it can read
(R17), and confirms management through permission advice; the authority's
answer decides, and a client never infers management from partial local
state. A client enrols
itself (above) before it syncs anything else. Offline, nothing runs; changes
stay pending until a manager is online.

**Adds.** The sync reads the relation's rows for each space it manages under
the application's ordinary read policy (R15), together with the space's own
records, in one ledger read (§0 item 3), so it acts only on accepted rows: a
membership row that is still pending, or is later refused, never produces a
grant. For each declared member that is not a recipient it stages an `add`
grant (an account at its current account epoch; a group, resolved through its
binding (R24), at the current epoch of its public key chain) and then
delivers the current epoch key to the new account's active devices and
recovery roots, or to the group's public key. A row the manager cannot read
only delays that member's add until a manager that can read it is online.
The sync never re-grants or delivers to a reset-pending recipient (§2.11),
and never makes a declared member that is reset-pending a recipient again by
removing and re-adding it; it lists the account for a manager's approval.
Adding an account that was never a recipient of the space binds its current
epoch whatever its generation, and sets its admitted generation; when that
generation is above 0 the same publication writes an admission notice
(§2.11). An account that was ever a recipient there and has reset since needs
an approval, whatever its current grants say.

**Removals happen only on confirmed deletion.** The sync never removes a
recipient because a row is merely absent from its view. It removes a
recipient `R` only after the authority confirms, through a **recipient
confirmation** request, that `R` is no longer entitled. The request answers
two questions about `R`, each `yes`, `no` or `unknown`: does any row of the
space's (or group's) membership relation still name `R`, and does `R` still
pass `manageKeys` on the governing row. The authority evaluates both over
every row, whatever the asking manager can read, with `R` (not the asker) as
the subject of the `manageKeys` check, and answers only to a key manager of
the space; the answers reveal nothing a manager is not already required to
read (R15) or entitled to act on. `R` is **gone** when both answers are `no`.
In manual mode only the second question applies, and a manager recipient that
also holds a current `member`-role `add` loses only its manager role. The sync
asks:

- live, when it observes a row naming `R` being deleted, or leaving the
  relation through an update;
- live, for every manager recipient, when it observes a change to the
  governing row or to another `manageKeys` input it can read;
- on catch-up, when it comes online or starts managing a space, once for every
  current recipient that no visible row names and for every manager
  recipient, so changes made while it was offline are found;
- before each rotation, by the rotating manager, for every manager recipient,
  so the new key never goes to a former manager.

When `R` is gone it stages a `remove` grant of each role `R` holds, then
rotates and delivers the new key to the remaining recipients (§3.4). When `R`
no longer passes `manageKeys` but a membership row still names it, it removes
only `R`'s `manager` role. Otherwise it does nothing (if a row it cannot see
names `R`, the client reports that the manager's view of the relation is
incomplete, a breach of R15); on `unknown` it retries later and keeps `R`.
Partial visibility therefore fails safe: no removal and no churn.

**Confirmation is bound to the publication that uses it.** A confirmation is
a premise, not a fact that stays true: `R` may be re-added, or regain the
right, between the answer and the commit. So the authority does not answer
for later use; it checks at commit. A publication that removes `R` on
confirmation, and a rotation that relies on the confirmations made before
it, carry their premises (for each subject account, the answers they relied
on, and the asking manager's own right). The authority re-evaluates every
premise against the state the transaction commits into, and refuses the
publication with `confirmation-stale` if any answer changed; the sync then
evaluates afresh. Ordinary content writes carry no premise.

**A rotation proceeds on `unknown`.** Before each rotation the rotating
manager confirms every manager recipient. A manager recipient whose answer is
`no` is removed first (in the same publication); one whose answer is `yes` or
`unknown` stays a recipient and receives the new key. For a rule the server
cannot evaluate for another account (one that reads session claims, which the
deploy accepts only with `"claims": "acknowledged"`, §1.5), every answer is
`unknown`: former managers are never removed automatically, and a manager
removes them explicitly (a `remove` of role `manager`, §3.4).

A manager recipient stays a recipient while it manages, even when no
membership row names it. When it loses the right, and no membership row names
it, the remaining managers' sync removes it as above; its own client may also
remove itself when its permission advice turns to denied (R26). The manager
running the sync is itself a manager recipient, so the sync never empties or
seals a space. Removing the last manager recipient is refused (R25).

**Maintenance.** Rotation requests (§3.1), stale recipients and missing
deliveries are handled by ordinary maintenance (§3.4) in the same run. A
pending rotation never blocks reads or writes (§3.4, §5.1).

**Idempotence and concurrent managers.** Several managers may run the sync at
once. A grant staged by the sync, or by self-enrolment, has a deterministic
ID: the version 8 UUID formed, as for space IDs (§3.2), from
`SHA-256(UTF8(json(["jazz.e2ee.space-grant-id.v1", spaceId, role, operation,
recipientKind, recipientId, n])))`, where `role` is the grant's `role` column
and `n` is the decimal count of accepted grants of the space with that role,
operation and recipient before it, whether replay applied them or not. A dead
row (one replay rejects) therefore still advances `n`, and the next attempt
steps over it. The server checks the ID against this derivation (§0) and
refuses any other version 8 ID, so a recipient cannot occupy a derived ID in
advance. (Group membership records use `"jazz.e2ee.group-membership-id.v1"` and
the group ID in the same way.) Two managers staging the same change produce
the same row ID, so the authority accepts one and refuses the other as an
existing row; the loser re-reads and finds nothing to do. Replay ignores an
`add` of a recipient already present at the same epoch and a `remove` of an
absent one (§3.4), so a duplicate that still lands is harmless. Concurrent
rotations resolve as in §3.4 (the first valid successor of a predecessor wins);
the loser re-reads, and its deliveries for the losing epoch are never used.

**Cost of churn.** One actor can drive rotation cost: a manager of a group
nested in many spaces can add and remove a member repeatedly, and every
removal obliges each ancestor to rotate and deliver; one device removal makes
every space and group of which the account is a direct recipient stale.
Coalescing (§3.1) bounds the rate, and rotation requests name their source
group, so ancestors' managers can see which child causes rotations (§8).

**Timing and what members see.** A new member is a declared member as soon as
its membership row is accepted, but holds no key until a manager has granted
it and delivered. Meanwhile its reads report the space as `key-pending`
(§4.3), not its rows as `key-unavailable`; when the delivery arrives, the
member fetches the key (§4.2), the report is cleared and the rows appear. On
removal, Jazz stops delivering the space's rows as soon as the membership
row's deletion is accepted (assuming the application's read policies follow
the same relation); the `remove` grant and rotation follow when a manager is
online. Keys already delivered are not revoked: the removed member can still
decrypt content of the epochs it held, but receives no key of the epoch the
rotation creates.

**Interactions.**

- **Self-delivery** (R22, §3.6) is unchanged. A member's own devices may
  deliver keys they hold to the member's other devices; the sync delivers to
  every active device of a new member anyway.
- **Groups as members.** A row naming a group makes the group a recipient; the
  space manager grants it and seals the key to the group's public key, and
  never reads the group's members (R20). The group's own members are kept by
  the group's managers: explicitly, or by this same sync when the group table
  declares `recipients`. A removal from a group writes the rotation request of
  §3.1 in the same transaction as the `remove` record, and the space's (or
  parent's) managers rotate in response.
- **Manual mode.** Without `recipients`, only self-enrolment of managers runs;
  the application adds and removes the other recipients explicitly (§3.4),
  and the last-manager rule (R25) still applies.

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

### 4.2 Key selection and membership

- A read decrypts with **any** key this device holds for the space, online or
  offline. Reads perform **no** membership check.
- Each cell names its epoch; the reader uses the held key of that epoch.
- A key is fetched (§3.5) only when the device holds no key for the space at
  all, or holds keys but a row needs an epoch it does not hold. The fetch
  happens at most once per read. When a fetch shows that an epoch is not in
  the space's lineage, the device remembers that until the chain it knows
  advances, and reports cells of that epoch `invalid-ciphertext` without
  fetching again; otherwise a live query pays the fetch again on each
  refresh.
- If the fetch reports "not a member", the read returns the rows its held
  keys decrypt and reports the rest `key-unavailable`. Any other fetch failure
  (pending delivery, unavailable, provider error, cancellation) fails the read,
  except for a declared member (below).
- **Declared members.** When the space table declares `recipients` (§1.5) and
  a row of the membership relation readable to this account names it (or names
  a group it is a current member of), the account is a declared member. If
  its fetch finds no grant yet, or a grant with no delivery yet, the read
  returns the rows its held keys decrypt, leaves the space's other rows out
  and reports the space once as `key-pending` (§4.3); it does not fail and
  reports no row `key-unavailable`. The device watches grants and deliveries
  addressed to it, fetches the key when one arrives, and live queries then
  receive the rows as ordinary additions.
- A held-key record that fails its checks (§6.2) is skipped and reported; the
  read continues with the other held keys and refetches that epoch's key if it
  can. (The TypeScript implementation failed every read of the space.)
- If the device holds no key for the space and cannot obtain one, the read
  fails.
- Keys obtained by a fetch, including earlier epochs from the history chain,
  become held keys.
- After a rotation, a device watches deliveries to **this device** (and group
  deliveries to its groups) for each held space; when a delivery of a new epoch syncs in, it fetches that key
  without waiting for a read. A failed fetch is retried (with backoff) until the
  key is held, a later epoch supersedes it, or the session closes. (The
  TypeScript implementation watched deliveries to any device of the account and
  attempted each epoch once.)

### 4.3 Unreadable rows

An ordinary query or subscription leaves out a row with an undecryptable
encrypted cell and returns the rest; no partial plaintext is returned. Report
`{table, id, column, code, space}`:

| Code                 | Meaning                                                                                                                                 |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `invalid-ciphertext` | Authentication failed; the cell belongs to another row, table, column or space; malformed; no such epoch in the space lineage           |
| `unsupported-format` | Unknown envelope mechanism or version                                                                                                   |
| `key-unavailable`    | This device lacks that epoch's key (held-key path)                                                                                      |
| `key-pending`        | Per space, not per row: this account is a declared member (§4.2) whose grant or delivery a key manager has not yet made                 |
| `space-pending`      | Per space, like `key-pending`: this account is a declared member of a space no key manager has created yet (§3.3)                       |
| `empty-cell`         | The stored cell is the empty byte string; the row is returned with the column's default (§4.1)                                          |
| `superseded`         | The cell holds the cleared marker (§4.1) and this reader's declaration names no conversion for the column                               |
| `stale-space`        | The cell was sealed under another space of the same row's history; it was read and needs repair ([migrations](E2EE_MIGRATIONS.md) §3.9) |

A `key-pending` or `space-pending` report carries `{table, space, code}` with
no `id` or `column`, and is deduplicated by `(table, space)`; it is not repeated for
each row. It is withdrawn when the key is fetched, and live queries then
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
column. A token is computed per held epoch; the physical predicate becomes
`__e2ee_eq_<column> IN tokens`. Limit and offset are applied after each
candidate is decrypted and its authenticated value compared. If the epoch set
changes during the query it is retried (shared budget of 3 attempts). Offline,
no tokens are computed: every candidate of the space is decrypted and
compared. Ordering by encrypted columns, traversals, unions and partial
selects are refused as unsupported; they never fall back to a scan.
Includes of encrypted child tables are refused too (a room list with its last
message takes two queries); developer documentation should say so.

**Cold queries.** Online, a query with an encrypted predicate on a space for
which this device holds no epoch key cannot compute any token, and an empty
`IN ()` would return a false empty result without ever revealing a missing
epoch. Planning therefore reports the missing space key, and the runtime
acquires it (§3.5) within the read's own budget before executing; acquisition
outcomes are those of §4.2 (a declared member waiting gets `key-pending` and
no rows). The query then runs against the epoch set it was planned with.

**Completeness.** An equality query returns matching rows of the epochs this
device holds. When the device knows (from the chain it last fetched) that the
space has epochs it does not hold, the result may miss rows of those epochs,
and the query reports the space once as `key-unavailable` so the result is
not mistaken for complete.

### 4.6 Subscriptions

A subscription that is an accepted current member with no delivery yet waits
without emitting, in one bounded episode (the operation timeout, 10 s by
default). Delivery or authority changes trigger fresh reads; there is no
polling and retries do not extend the deadline. On expiry the subscription
fails with `TimeoutError` "Encrypted query timed out waiting for a space key
delivery". One-shot reads do not wait.

While it waits, the subscription delivers nothing, but core keeps computing
deltas against its own baseline. The subscription wrapper therefore keeps a
full copy of the latest result (applying each withheld delta to it) and, when
the wait ends, emits that whole result as one reset (a snapshot replacing
whatever the subscriber had), after which it forwards deltas unchanged.
Boundaries: on key arrival or, for a declared member, on expiry, it emits the
reset; on expiry otherwise it fails and discards the copy; on stop, sign-out
or account change it discards the copy and emits nothing; a held-key removal
during the wait changes nothing until core's next refresh, whose delta the
copy absorbs. For a declared member (§4.2) the
episode also covers a grant that has not been made yet, and expiry does not
fail the subscription: it emits the rows its held keys decrypt, reports the
space `key-pending`, and keeps watching; the rows arrive as additions once
the key is fetched. A manager may be offline for much longer than one
episode, so a declared member's wait is reported, not treated as an error.

## 5. Writes

### 5.1 Held key permits the write

A held key permits encrypting and submitting an ordinary Jazz write. The
content-write path adds no check of current membership, device approval,
epoch freshness or Global acceptance; only Jazz permissions accept or reject
the write. Key binding, session lifetime and cancellation still apply. When no
key is held, key acquisition (§3.5) happens before the write proceeds.

A pending rotation (§3.4) never blocks a write: until a manager rotates, the
device writes with the newest key it holds (§5.2), even when that epoch is
due to be replaced.

Deletes, and updates or restores that touch no encrypted column, need no key. Partial edits of encrypted columns are refused.
An update re-encrypts only the supplied encrypted columns, so one row may
carry cells of different epochs. An encrypted row cannot change its space
coordinate through an ordinary write. The one exception is the `move` write
of [migrations](E2EE_MIGRATIONS.md) §4.4: a serializable, online-only write
that re-encrypts every encrypted column under the target space and changes
the space column in one transaction, with held keys of both spaces.

### 5.2 Which epoch

A write uses the newest authenticated key available to this device: the held
key with the greatest lineage depth. A held key's `position` always means its
epoch's lineage depth (root = 0); one lineage orders by depth alone, so the
ledger position is not needed. (The TypeScript implementation stored the ledger
position or a chain-relative index on some fetch paths.) When acquiring, the device starts from
the current epoch and, if it cannot open that epoch's key (for example the
successor's delivery has not arrived), steps back through predecessors; an
undelivered successor therefore does not block writes. Equality tokens use
the same key and epoch.

### 5.3 Offline and provisional writes

A write is provisional exactly when reads would use retained local state.
With a held key, an offline write records no membership predicate. Governed
offline transactions (space creation, membership changes) keep the author's
device-active predicate (§3.7).

### 5.4 Space creation, including offline

An encrypted write to a space with no root, by a session that manages the
space, creates the space (§3.3) in the same transaction; a session that does
not manage it gets `space-pending`. Inserting a self-scoped space row is the
common case. The new key is stored as a held key before commit, with
a persisted `unpublished` mark (§6.2), with the creation's durable
transaction identity, that is cleared once the creation is accepted; an unpublished key never wins a tie against a published key at the
same depth, including after a restart. It is discarded only when the creation is
definitively not accepted (refused at Local or Global, or cancelled before
possible submission): the epoch is fenced in memory, leases erased, and only
the record matching this device and exactly that root, epoch and signature is
removed. An unknown outcome keeps it. After a restart the runtime reconciles
each `unpublished` record by its stored transaction identity
(`transaction_fate`): accepted clears the mark, refused discards the record,
pending and unknown keep it; if the identity is unknown to the store and an
authenticated root with this ID is readable, the mark is cleared when it is
this record's root, and the record is discarded when another root won. If a different authenticated root wins
at the same depth, the unpublished key loses.
(The TypeScript implementation kept the mark in memory only.)

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
- Space creation commits with its first rows.
- A membership change commits with the application rows staged with it.
- **Keys inside a callback transaction.** A staging call that needs a key
  this device does not hold returns "needs key". In a binding whose
  transaction callback is asynchronous, the staging call awaits acquisition
  (§3.5) and retries; the callback itself is not replayed. The synchronous
  `Db::transaction(cb)` cannot await, so such a staging call fails with
  `key-unavailable` and the transaction aborts; the application acquires keys
  first with `db.encryption.acquire(space)` and runs the transaction again.
  (The TypeScript implementation had "content-key declarations" and refused
  one that overlapped a membership change; both are dropped.)
- Unknown outcomes are reconciled by the original transaction's identity, not
  by resubmitting.

## 6. Private local state

### 6.1 Store

Private state is kept in a store separate from authentication storage. Per
scope `Dₐ(account)` it holds one device document and one held-key document
per space (or group) the device holds keys for, so storing a key rewrites
only that space's document:

```text
device document:
{ "format": "jazz-e2ee-local-devices-v2",
  "devices": [ { scope, id, mechanism, version, publicKey, privateKey,
                 signingMechanism, signingVersion,
                 signingPublicKey, signingPrivateKey } … ] }

held-key document (one per space or group root):
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
small record per held epoch that refers to it by depth:

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
(its own derived ID and creator domain); `successors` are the accepted
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
current device, its signature verifies, the root decodes and has the derived
space ID of its address, and `successors` is a chain (each successor's
`predecessor` is the previous epoch, starting at the root's epoch, epochs
unused). A held-key record is accepted only if: its binding matches; its
signature verifies; `rootId` is the lineage's root; `depth` is at most the
lineage length, `prefix` equals the hash of the lineage prefix of that depth
and `epochId` is the epoch at that depth (the root's at depth 0); the
envelope opens; and the marker of `epochId` is valid for the opened key: the
root's `verification` (`X("verification")` under the creator's domain) at
depth 0, otherwise the `verification` of the successor at that depth
(`X("verification", "", epochId)`). The signatures and author activity of
the lineage were checked when the key was fetched (§3.5); the device's own
signatures protect them since. A record that fails is skipped and reported,
never fatal to the read (§4.2); a failed lineage record skips the space's
keys and the space is fetched again. Nothing in either record depends on the
application schema, so a schema update never invalidates held keys; the cell
context binds table, column and type (§4.1). Keeping a key replaces any
record of the same device, root and epoch. `unpublished` is present only for
a space creation not yet accepted (§5.4), carries the creation's durable
transaction identity for reconciliation after a restart, and is removed by
rewriting the record. The lineage records also give the runtime its
epoch-to-space lookup ([migrations](E2EE_MIGRATIONS.md) §3.9).

**Groups** use a variant of the lineage record, because a group's event chain
is its public key chain and may contain unusable epochs (§3.1):

```text
{ format: "jazz-e2ee-held-group-lineage-v1",
  binding: { … as above },
  groupRoot: <group root row>,
  events: [ <group public-key record>, … ],
  successors: [ [ epochId, <group successor row> ], … ],
  signature: [..] }
```

`events` are the public-key records of the chain in order, from the root
record; `successors` holds the private successor row of each usable epoch
held (unusable epochs have none). A held-key record of a group names
`rootId` = the group ID and its `epochId`; `depth` is the event index and
`prefix` hashes `[groupRoot, events[0 .. depth]]`. On load: the binding and
signature as above; the group root row has the record's group ID (group IDs
are random, so no derivation is checked); `events` form a chain by the
public key chain rule (§3.1) as far as it is stored; each stored successor
has the same ID, predecessor, proof epoch and author as its event; the
marker of `epochId` uses `G("verification")` at that epoch from the group
root (depth 0) or that epoch's successor; and an epoch whose event has no
stored successor holds no key, except the root epoch, whose key is checked
against the group root's marker. Key links follow each successor's
`proofEpochId`, so a restarted device holds `K2` and `K0` and knows `G1` as
unusable in the example of §3.1.

(The TypeScript implementation's `jazz-e2ee-held-key-v1` record bound a
`schema` hash, which it signed but did not check, stored a position instead
of the epoch and its lineage, had no persisted `unpublished` field, and
failed the read on an invalid record.)

### 6.3 Binding

All private records are bound to the application id, the environment and the
account (`Dₐ(account)`), not to the server URL. One install pointed at two
datasets with the same application id and environment shares device keys and
held keys.

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
- Membership changes never remove held keys. Records are removed only by
  sign-out wipe, account change wipe, device removal, explicit deletion, and
  discard of a failed space creation (§5.4). A wiped store is never recreated
  by a late operation.
- A held-key revision counter advances on every removal, so in-flight reads
  notice and retry (3 attempts).

## 7. Failure taxonomy

Every operation keeps its original error. Publication errors carry the
original record IDs and, when the refusal has a specific cause, that cause's
code (for example `recovery-root-mismatch`); a specific failure is never
flattened into a bare `refused`. The data-error codes are exactly
`key-unavailable`, `key-pending`, `space-pending`, `unsupported-format`,
`invalid-ciphertext`, `empty-cell`, `superseded`, `stale-space` and
`encryption-failed`. Encryption status events (not data errors) are
`account-reset` and `reset-approval-needed` (§2.11) and key requests to
answer (§3.4).
Every reader from the first release knows all of them, so that rows written
by a later schema version degrade to a reported gap. An equality query on a
space whose conversion or tag backfill is unfinished fails with the query
error `conversion-pending` ([migrations](E2EE_MIGRATIONS.md) §3.3), which
names the table, column, space and pending count.

| Outcome                     | TypeScript form                                                                                                                                                                                  | Must                                                                                  | Must not                                                                                       |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| Device approval pending     | `AccountKeyUnavailable` / readiness `not-verified`                                                                                                                                               | Show "waiting for approval or keys"; complete approval or recovery                    | Re-approve, or treat the account as signed out                                                 |
| Key unavailable             | `E2eeDataError("key-unavailable")`, `SpaceKeyUnavailable`, `GroupKeyUnavailable`                                                                                                                 | Check readiness and delivery                                                          | Write plaintext                                                                                |
| Delivery pending            | `SpaceKeyDeliveryPending`                                                                                                                                                                        | Subscriptions wait one bounded episode                                                | Poll, or emit a partial result                                                                 |
| Key pending                 | None (declared membership relations are new)                                                                                                                                                     | Report the space `key-pending`; show "waiting for key"; keep watching                 | Report each row `key-unavailable`, or fail the read                                            |
| Space pending               | None (new)                                                                                                                                                                                       | Refuse the write `space-pending`; report a declared member's read; watch for the root | Create the space without the right, or write plaintext                                         |
| Superseded / stale space    | None (new, schema conversions)                                                                                                                                                                   | Report; leave a `superseded` row out; return a `stale-space` row                      | Report either as `invalid-ciphertext`                                                          |
| Not a member                | `SpaceKeyNotMember`                                                                                                                                                                              | Return what held keys decrypt; report the rest                                        | Fetch again within the read                                                                    |
| Permission refusal          | Jazz rejection; `*PublicationError` outcome `refused`                                                                                                                                            | Report; reflect in local state                                                        | Treat key possession as overriding it                                                          |
| Authentication failure      | `E2eeDataError("invalid-ciphertext")`, `InvalidKeyEnvelopeError`                                                                                                                                 | Leave the row out (ordinary reads) or fail (strict reads)                             | Return unverified plaintext                                                                    |
| Unsupported format          | `E2eeDataError("unsupported-format")`                                                                                                                                                            | Report                                                                                | Treat as invalid ciphertext                                                                    |
| Encryption failed           | `E2eeDataError("encryption-failed")`                                                                                                                                                             | Fail the write                                                                        | Submit plaintext                                                                               |
| Provider failure            | The mechanism's `CryptoFailure` (distinct from `InvalidCiphertext`, `InvalidInput`, `NoKeyForMechanism` and a `false` verification), and a writer's failure to read back output it just produced | Fail the operation loudly; preserve the error                                         | Report it as key unavailable, corrupted ciphertext or an invalid proof; try the next candidate |
| Unsupported capability      | `UnsupportedE2EECapabilityError` (`ledger-version`, `signing-mechanism`, `signing-version`)                                                                                                      | Leave the dependent state unresolved                                                  | Treat the record as invalid                                                                    |
| Intrinsic contradiction     | `IntrinsicAccountHistoryError` (`conflicting-roots`, `conflicting-device-keys`, `missing-root-key`, `missing-recipient-key`, `missing-signer-key`)                                               | Fail current replay; skip only a candidate decided at a fixed signed cutoff           | Pick one of the conflicting rows                                                               |
| Unresolved group history    | `GroupHistoricalUncertaintyError`, `GroupAccountReadinessError`                                                                                                                                  | Report the uncertain group to its own members and managers                            | Bypass the record                                                                              |
| Unknown publication outcome | `AccountPublicationError` / `GroupPublicationError` / `SpacePublicationError` outcome `unknown`                                                                                                  | Reconcile the original operation by its record IDs                                    | Resubmit blindly or replay a callback or source                                                |
| Recovery failures           | `E2eeRecoveryError`: `recovery-cancelled`, `recovery-material-unusable`, `recovery-root-mismatch`, `recovery-delivery-missing`, `recovery-delivery-unusable`                                     | Only candidate failures permit trying another delivery                                | Attach private material to the error                                                           |
| Unsupported shape           | Typed "Unsupported encrypted query…" errors                                                                                                                                                      | Name the operation                                                                    | Fall back to a scan                                                                            |
| Timeout / cancellation      | `TimeoutError`, `AbortError`                                                                                                                                                                     | Erase owned secrets; stop unsubmitted work                                            | Claim a submitted operation failed                                                             |

Publication stages: account `request`, `bootstrap`, `challenge`, `proof`,
`approval`, `account-rotation`, `recovery-root`, `recovery-delivery`,
`account-reset`; group `inspect`, `membership`, `create`, `root`,
`initial-delivery`, `rotation`, `delivery`, `repair`, `maintain`,
`inspect-recovery`, `restore-recovery`; space `inspect`, `create`,
`membership`, `maintenance`, `delivery`, `repair`, `discover`, `deadline`,
`inspect-recovery`, `write`.

Serialization and transaction conflicts (including `confirmation-stale`) are
retried internally only by operations that are safe to repeat because they
re-read their evidence and stage fresh rows: the runtime's own governance
publications (enrolment, recipient sync, maintenance, delivery, repair,
discovery and creation of spaces), conversion sweeps, and a staging call
retried after "needs key" before submission (§5.6). A public mutation or
transaction callback is never retried or replayed: its conflict is returned
to the caller, and an unknown outcome is reconciled by its transaction
identity.

(The TypeScript implementation wrapped every failure inside a publication as
a bare `refused` with the stage name, and defined two data-error codes,
`key-not-shared` and `maintenance-required`, that nothing raised; both are
dropped.)

Refusals that had no code in the TypeScript implementation (only a message)
get these codes:

| Code                          | Refusal                                                                                                                                              |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `account-has-history`         | Bootstrap found existing account evidence (§2.3).                                                                                                    |
| `device-identity-conflict`    | A device request or key row exists with different keys (§2.2).                                                                                       |
| `device-keys-mismatch`        | The device's keys do not match its accepted request.                                                                                                 |
| `no-device-request`           | Bootstrap or reset found no request (or more than one) for this device (§2.3, §2.11).                                                                |
| `reset-device-active`         | An account reset by a device that is still active in the current generation (§2.11).                                                                 |
| `device-not-active`           | The acting device is not an active device of the account.                                                                                            |
| `device-removed`              | The device was removed and needs a new device ID.                                                                                                    |
| `stale-challenge`             | The challenge is unknown, for another device, or not of the current account epoch (§2.5).                                                            |
| `approval-closed`             | The approval handle was closed or belongs to another operation.                                                                                      |
| `invalid-possession-proof`    | The requesting device's possession proof failed (§2.5).                                                                                              |
| `target-not-active`           | The device or recovery root to remove or retire is not active (§2.8).                                                                                |
| `unsupported-recipient`       | A recipient device or recovery root uses an unsupported mechanism and cannot be required.                                                            |
| `space-sealed`                | The space is sealed (§3.4).                                                                                                                          |
| `group-sealed`                | The group is sealed (§3.1).                                                                                                                          |
| `recipient-absent`            | A removal names a recipient that is not present.                                                                                                     |
| `recipient-needs-maintenance` | A recipient's account epoch changed; the space or group must rotate first.                                                                           |
| `group-unavailable`           | A group to add has no public-key record.                                                                                                             |
| `account-reset-not-allowed`   | An account reset by a local-first session, or where the application has not enabled `allowAccountReset` (§2.11).                                     |
| `space-pending`               | An encrypted write to a space that no key manager has created yet, by a session that does not manage it (§3.3).                                      |
| `confirmation-stale`          | A premise of a recipient confirmation changed before the publication committed (§3.8).                                                               |
| `recipient-still-declared`    | A self-removal of the member role while a membership row still names the account (R26).                                                              |
| `recipients-declared`         | An explicit recipient change for a space or group whose recipients follow a declared relation (§3.4).                                                |
| `last-manager`                | The grants or membership records would leave a space or group with no manager recipient (R25), or the SDK recognised that leaving or demoting would. |

## 8. Security properties and non-goals

Properties:

- Only devices that obtained an epoch key can read cells of that epoch.
  Cells, tokens and streams are bound to application, environment, space,
  table, row, column, epoch and logical type; moving ciphertext between any of
  these fails authentication.
- Membership, approval, rotation and delivery records are signed by an active
  device of their author and replayed in accepted order; forged or reordered
  records are skipped, contradictions fail closed.
- Removal plus rotation excludes the removed account or device from keys of
  later epochs, except a device of a local-first account, which holds the
  account secret (see the limits below). Each account rotation and space
  rotation generates a fresh key. An ancestor's new key is never sealed to a
  child key the removed member holds (§3.1, delivery floor).
- Unsupported or unverifiable evidence is never treated as invalid, and
  provider errors are never treated as proof of invalidity.
- Error messages carry fixed text and public coordinates, never plaintext,
  keys or provider diagnostics.

Non-goals and explicit limits:

- **The server is trusted to authorise; encryption does not check it.** The
  protocol protects content against a server that stores and reads
  faithfully, and against disclosure of its database or backups. It does not
  protect against a server, an operator or an application policy defect that
  accepts a forged membership row or answers `manageKeys` wrongly, because
  clients verify authorship and signatures, not authorisation (§1.4). Each of
  these yields keys:
  - in declared mode, a membership row naming account X is accepted; the next
    key manager's sync grants X and delivers the current key to its devices
    (§3.8);
  - account X is answered as passing `manageKeys`; X enrols itself as a
    manager recipient, and any manager holding the key delivers to it (§3.8);
  - the unsigned account root of an account that has not bootstrapped is
    written by someone who can write as that account; managers deliver to
    that device (§2.3), and the same holds for an account reset (§2.11).
  - an `admitted` notice for a reset-pending account is accepted outside the
    transaction of the root or of the account's first `add` (§2.11); the
    notice form of the pending-author rule (§3.1) cannot detect it, so
    clients then accept that account's governance records as if a manager
    had approved its reset.

  Possible later work: clients could show members when a space's recipient
  set changes, so that an unexpected recipient is noticed.

- **Removal cannot erase copies.** A delivered key is never revoked: removal
  withholds future keys (by rotation) and Jazz stops delivering future rows.
  Keys, plaintext and ciphertext a former member already obtained remain with
  them, and old epochs stay readable to whoever holds their keys.
- **Read access is Jazz's.** Encryption does not stop a removed member from
  fetching ciphertext; only Jazz permissions do. Key possession grants no
  permission, and administration of groups and spaces is enforced only by
  Jazz policy (R11–R27).
- **Writes with a held key are not freshness-checked.** A device holding an
  older epoch's key may write under it until Jazz permissions refuse.
- **Self-only reads hide metadata from other users, not from the server.**
  The server stores and evaluates every E2EE row, so it sees who is a
  recipient or member of which space and group, who manages them, and when
  each rotates; R11, R12 and R17 only keep other users from reading them.
- **Metadata is visible to the server and to readers permitted by policy:**
  which rows, spaces, groups and devices exist; row IDs, relationships, space
  coordinates, authorship and timing; ciphertext sizes; key and membership
  records; equality-token repetition within one epoch's domain. Group
  public-key records are readable by every signed-in session: they reveal
  that a random group ID exists, when it rotates and which account (a manager)
  published each epoch, but not which application row it belongs to or its
  members. The public account tables (R10) are readable by every signed-in
  session too: anyone can see how many devices an account has enrolled and
  removed, when, and whether recovery is set up. Self-only reads (R11, R17)
  apply to space and group records, not to these. Space IDs are derived
  from the governing row's table identity and row ID, so anyone who knows a row
  ID can recognise that space's records (space records are readable only by
  its recipients and managers).
- **Removing a device of a local-first account does not exclude it.** For a
  local-first account, whoever holds the account secret is the account. A
  removed device that still holds the secret signs in as the account, keeps
  receiving its rows, re-enrols through recovery (§2.9) and decrypts. The
  remedy for a lost or stolen local-first device is a new account. Device
  removal remains effective for accounts authenticated by the application's
  own sign-in (§2.8).
- **Account reset rests on managers' approval.** For an application that
  enables `allowAccountReset`, whoever can sign in as an app-authenticated
  account can reset it. In every space and group where the account held a
  role before the reset, it then receives no key and cannot govern until a
  key manager there approves the reset; an approval delivers the current key
  and with it the whole (usable) history. The approval is the check against
  a hijacked application login, so managers should confirm with the person
  out of band before approving. An account that reset before it ever entered
  a space or group (even if it was already declared there or held
  `manageKeys`) is admitted afterwards like a new member, without approval.
  A sole manager that resets leaves the space or group with no usable
  manager until the application gives someone else the right; the security
  documentation should say so. Spaces and groups the account first joins
  after the reset admit it like any new member, with an admission notice;
  access it has only through a group follows that group's own approval.
  Members see a verified approval naming the account (§2.11), which also
  tells them that the account is a recipient. Local-first accounts cannot
  reset.
- **Removal from a nested group takes effect at the parents' rotations.** A
  member removed from a child group keeps the parent and space epoch keys it
  already held, and they stay usable for content of their epochs; it receives
  no key of the epochs each parent or space creates when it rotates in
  response to the rotation requests, because no ancestor seals a new key to a
  child key from before the removal (§3.1). Jazz permissions stop row
  delivery to it immediately. After a device removal, a parent may seal to
  the group's previous key until the group itself rotates (§3.1).
- **Waiting for a child's coverage delays removal.** An ancestor that cannot
  yet see a child's key shown clear rotates at once by default (§3.1). If an
  application configures a coverage wait, a member removed from a nested
  group can read the ancestor's current epoch for up to that interval; the
  wait only saves the ancestor one rotation when the child turns out to be
  clear.
- **Declared recipients converge only while a manager is online.** Between a
  membership change and a key manager's next sync, a new member waits for its
  key and a removed member still holds the current epoch's key (but receives
  no rows from Jazz).
- **Managers read what they manage.** Every key manager is a recipient
  (§3.8). An application backend configured as a manager holds the keys, and
  those spaces are then not end-to-end encrypted against it.
- **Orphaned spaces and groups.** Without a manager recipient a space or group
  cannot add members or rotate. The server refuses only removing the last
  manager recipient's grant (R25); an application data edit that takes the
  right from every manager, a session claim, or a change outside Jazz can
  still orphan a space. Documentation should recommend at least two managers
  per space and group.
- **A manager recipient is not proof of a usable key holder.** R25 counts
  enrolments, not keys: a newly enrolled manager may still wait for its first
  delivery when the previous key holder leaves, and a manager may lose its
  devices. The SDK shows, during a manager handover, whether the remaining
  managers hold the current key and have recovery set up; a member holding
  the key can restore it to managers (R27).
- **Members wait for a manager.** A space whose only manager is rarely online
  admits nobody, rotates late and creates no spaces for new governing rows.
  After a removal from a child group, an ancestor that rotates first withholds
  its new key from the whole child, so the child's remaining members cannot
  read the ancestor's new content until a manager of the child rotates
  (§3.1).
  Documentation should lead with this.
- **One actor can drive rotation cost.** A manager of a group nested in many
  spaces can force every ancestor to rotate by adding and removing a member
  repeatedly, and one device removal makes every space and group of which the
  account is a direct recipient stale (§3.8). Coalescing bounds the rate;
  ancestors see the source group and can remove it.
- **Any space manager can grant any group.** Group IDs are public, so a
  manager can make an unrelated group a recipient of its space; that group's
  members then see the space's root and chain. Confidentiality is unaffected,
  but the grant is unsolicited.
- **Key versions accumulate with churn.** Each removal adds an epoch; reads
  and equality queries work per held epoch, and each rotation costs one
  delivery per remaining direct recipient device or group (§3.4).
- **Rotation does not re-encrypt old content**, and a pending rotation does
  not stop clients from writing under the epoch it will replace (§3.4, §5.1).
- **Local-first recovery is as strong as the account secret.** The E2EE
  recovery key of a local-first account is derived from its account secret
  (§2.9), so whoever holds that secret (any device that stores it, a script
  on an origin that keeps it in `localStorage`, or anyone with its phrase or
  passkey) can restore the account's E2EE keys. Passkey backups are protected
  by the platform's user verification.
- **No side-channel padding.** Ciphertext length reveals plaintext length.
- Local private records are protected only by the platform store; scripts on
  the same origin can read them.
- Integrity of encrypted cells is not checked by the server: Jazz stores any
  bytes a permitted writer submits; readers detect and leave out bad cells.
- **Internal records are never deleted** (R1). Delivery rows grow with every
  rotation and device, and a deleted governing row or account leaves its
  records. This is accepted for the first release; the cost is to be
  measured with the key-version growth above.
