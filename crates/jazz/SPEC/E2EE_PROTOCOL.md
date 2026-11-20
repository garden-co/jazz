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
`tasks`). Jazz provides one **members table**: a row `(account, space)` makes
that account a **member** of the space. The space row's read rule must come
from it, so the members of a space are exactly the accounts entitled to its
keys, and members can see who the other members are. Each space has a chain
of key versions, each linking back to the previous one, so the newest key
opens the whole history. Members' clients derive all key work from tables
they can read: a key holder delivers the newest key to every member that
lacks it, sealed to that member's account, and a key held by an account that
is no longer a member marks the space as needing a new key. Only sessions
that may read and update the space row change its members, create the space
or rotate its key, so the space row's update rule is the rule for managing
its members. There are two exceptions for membership: the account that
creates the space row may add itself in the same transaction, and any
member may leave. The
first release supports up to 1,000 members per space (§3.8).
Encryption keeps content unreadable outside Jazz's permissions; it
adds no access rule of its own (§8).

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
- **Selection migration and retirement are different operations.** A
  _selection migration_ switches the mechanism new records use (below); old
  records keep theirs and stay readable while it stays listed. _Retiring_ a
  mechanism (for example after a weakness is found) is removing it from the
  allow-list. Retirement is a breaking change, by design, and takes effect
  at once: every record that depends on it becomes unreadable
  (`unsupported-format`) and every signed chain that depends on it
  unverifiable (an unsupported capability, §2.4 step 4, §7), for every
  reader of the application. There is no grace period, fallback or partial
  acceptance. Writers can select only listed mechanisms.
- **What a migration lets a developer retire.** After a selection
  migration, rotations and re-encryption move _content_ and _new key
  versions_ onto the new mechanism: a content mechanism (cell cipher,
  equality tokens, streams) can be retired once every cell, token and stream
  that must stay readable has been rewritten under the new one. They do
  **not** remove a chain's dependence on its earlier links. Account roots,
  approvals, successors and recovery roots, and space roots, successors,
  markers and `history` wraps are immutable, and every key is authenticated
  by walking its chain back to the root (§2.7, §3.3): a fresh device that
  obtains only the newest space key still verifies the signatures and
  unwraps the history and markers of every earlier link. A signature or key
  wrapping mechanism used anywhere in a chain therefore stays required for
  as long as that chain is used, and retiring it makes the chain
  unverifiable, including content that was fully rewritten under new
  versions of it. Rotation does not change this. Retiring such a
  chain-authentication mechanism needs new chains (new accounts and new
  spaces, with the content copied into them), which this protocol does not
  specify; until then those mechanisms must stay allowed. A sealing
  mechanism is needed only to open envelopes sealed with it; whether
  anything still needed is sealed only under it is not tracked by the
  protocol.
- **The allow-list is configuration.** A change of the effective allow-list
  is a configuration change (§2.10): it closes the live encryption owner,
  which drops every opened key, replayed chain and decrypted-cell cache held
  in memory ([core hooks](E2EE_CORE_HOOKS.md) H3). The next owner checks
  every record and held-key record again under the new list, so nothing
  opened under the old list bypasses a retirement.
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
- Selection migration: ship the new mechanism to every client, then switch
  the configuration. Each client then enrols a new device identity of the
  new mechanism and removes its old one; recovery secrets register their
  second root; each account publishes its sealing key of the new mechanism;
  the next rotation of each space wraps its new key with it. Until an
  account has a sealing key of the selected mechanism, sealing to it fails
  with "recipient unsupported" and deliveries to it wait. Old records are
  not rewritten, and the old mechanism stays on the allow-list (above).

### Identifiers

Every record ID, epoch ID, device ID and recovery root ID is a UUID,
generated at random (version 4 or 7), except a recovery root ID derived from
a recovery secret (§2.9), which is version 8, and a generation-0 account
identity, whose ID is its account ID (§1.2). Signed statements and contexts
use an ID's canonical lowercase hyphenated text and check it against
`^[0-9a-f]{8}-[0-9a-f]{4}-[47][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`
(versions 4 or 7); recovery root IDs accept versions 4, 7 and 8. Account IDs
are the authenticated Jazz account ID, which is always a UUID (a version 5
UUID from Jazz's account registry; a device account's ID is derived
from its device-account public key, §2.3); account IDs are checked as UUIDs of
any version. The internal tables store every ID and account ID as `uuid`
(§1.2), so that policies compare them with the session account, itself a
UUID, without conversion.

**Secret-derived recovery root IDs** (§2.9) are keyed BLAKE2b from the
recovery seed (which binds the account generation above 0), the first 16
bytes with `b[6] = (b[6] & 0x0f) | 0x80` and `b[8] = (b[8] & 0x3f) | 0x80`.
The server cannot recompute them; it checks only R9 and that the ID is not
already used. Only the account itself may register a root (R9), so a wrong
ID can only break that account's own recovery, which the client detects by
deriving again. No other ID is derived: reset identities, member rows and
deliveries have random IDs, so nobody can predict, and occupy in advance, the
ID of another account's row.

Secret-derived recovery root fixtures, for generations 0 and above, are in
§2.9; the account sealing key fixture is in §2.12.
`crates/jazz-e2ee/tests/protocol_vectors.rs` pins the recovery derivation,
the account sealing key derivation and the SHA-256 digest of every record
kind's signed bytes, each rebuilt there from this document's formulas; those
digests pin this document's reading, not an independent implementation's.
An insert refused because a row with that ID already exists is
reported to the writer exactly like a policy refusal.

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
account. No URL or registry path is parsed.
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
on Jazz for the following; which of them core provides today and which must
be built is stated in the [core hooks note](E2EE_CORE_HOOKS.md) (§4 there):

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
   atomically, at one position. Account operations, rotation and an explicit
   `createSpace` wait for Global acceptance (the authority's decision), not
   only Local; a space creation staged with an application write follows
   that write's own durability (§3.2).
5. **Read-set validation.** The authority refuses to commit a ledger
   transaction (with a conflict, not a policy refusal) when any evidence the
   transaction read at its cut has changed by its commit: a row it read was
   superseded in a way the read would have returned, or a row matching one of
   its reads was accepted after the cut. The protocol relies on this for one
   accepted bootstrap or reset per account and generation (§2.3, §2.11), one
   accepted successor per predecessor (§2.4, §3.5) among honest online
   writers, and deliveries sealed to the recipient's current account epoch
   (§3.4); replay decides when more than one is accepted anyway. Space
   roots are not unique (§3.2).
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
  **recovery secret** (§2.9): for a device account, the account secret
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
- **Member, updater.** A member of a space is an account with a row for the
  space in the members table (§1.3); members may obtain the space's keys. An
  updater is a session that Jazz lets read and update the space row
  (read(row) and update(row), §1.4); updaters change the space's members,
  create the space and rotate its key. The space row's read rule comes from
  the members table (R13), so every updater is a member. Jazz permissions
  alone decide both. **The space row's update rule is therefore the rule for
  who manages members:** whoever may update the space row may add any
  account and remove any member. Membership administration cannot be
  separated from editing the space row, so fields that members other than
  the managers may change belong in another table (§1.5).
- **Space creator.** The account that creates the space row (its
  `$createdBy.account`). It may add itself as a member in the transaction
  that inserts the space row, and at no other time (R12). After that it is
  a member like any other: removing it removes it.
- **Key version.** A space's key history is a chain: the root version, then
  one per **rotation** (a successor). Each version has an epoch ID and a
  random 32-byte key, and wraps the previous version's key, so the newest key
  opens every earlier one. The one exception is a recovery rotation (§3.5),
  which wraps the newest version its author holds and so skips the versions
  after it.
- **Delivery.** One key version sealed to one member's account sealing key
  (§2.12), written by a member that holds it (§3.4).
- **Key holder.** An account that holds a version, as the accepted records
  show it: it wrote the root or successor of that version (its
  `accountEnvelope`), or a delivery of it to the account was accepted. This
  is conservative evidence from envelope metadata, not proof of possession:
  the server checks no envelope, so an accepted delivery whose envelope does
  not open still counts (§3.5).
- **Held key.** A space key this device has authenticated and stores in its
  private store (§6). A held key is sufficient to read and write.

### 1.2 Account and device tables

All E2EE tables (this section and §1.3) are internal Jazz tables, like Jazz's
own account tables: they never appear as entries of the application's
`jazz.app.json` or `jazz.permissions.json`, and the application cannot define
or override their policies. The tables in this section have fixed policies (§1.4). "Public" means readable by any session in `device-account` or
`external` auth mode; "private" means readable only by the row's
`$createdBy.account`. Every row is immutable.

**Column types** (this section and §1.3). Row IDs, every column holding a
record, epoch, device, recovery root, delivery or account ID, and the space address
columns `scopeId` (a catalogue table identity, which is a UUID) and
`identifier` (a row ID) are `uuid`; mechanism IDs and the enumerated columns
are strings; keys, envelopes, markers, signatures, and canonical JSON
columns (`membership`, `revision`, `deliveries`) are bytes; `version`,
`signingVersion` and `ledgerVersion` are unsigned 32-bit integers (the
common envelope's version width);
`generation` is an unsigned 64-bit integer, written as a decimal string
wherever it enters JSON. Optional columns
(`recoveryRootId?`, …) are nullable.

| Table                              | Columns                                                                                                                                                                                   | Row ID          | Visibility | Meaning and writer                                                                                      |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------- | ---------- | ------------------------------------------------------------------------------------------------------- |
| `__e2ee_device_requests`           | `publicKey`, `mechanism`, `version`, `signingPublicKey`, `signingMechanism`, `signingVersion`, `nonceCommitment`                                                                          | device ID       | private    | A device's identity, written by that device.                                                            |
| `__e2ee_device_keys`               | `deviceId` and the six key columns of the request                                                                                                                                         | random          | public     | Public projection of the request's keys; must equal it. Written by the device.                          |
| `__e2ee_account_identities`        | `ledgerVersion` (1), `generation`, `deviceId`, `epochId`, `envelope`, `verification`                                                                                                      | identity ID     | private    | Private account root: root epoch key sealed to the first device, plus its marker. Written at bootstrap. |
| `__e2ee_account_roots`             | `ledgerVersion` (1), `generation`, `accountId`, `deviceId`, `epochId`, `kind`, `deviceAccountPublicKey?`, `deviceAccountSignature?`                                                       | random          | public     | Public account root. Written at bootstrap in the same transaction as the identity.                      |
| `__e2ee_device_challenges`         | `deviceId`, `epochId`, `envelope`, `nonce`, `signerId`, `signature`                                                                                                                       | challenge ID    | private    | An approval challenge to a requesting device. Written by the approving device.                          |
| `__e2ee_device_proofs`             | `challengeId`, `proof`, `nonce`, `signature`                                                                                                                                              | challenge ID    | private    | The requesting device's possession proof.                                                               |
| `__e2ee_device_approvals`          | `challengeId`, `signerId`, `verification`, `signature`                                                                                                                                    | challenge ID    | private    | Private approval with its account-key marker.                                                           |
| `__e2ee_device_deliveries`         | `challengeId`, `envelope`, `verification`                                                                                                                                                 | challenge ID    | private    | Account epoch key sealed to the approved device.                                                        |
| `__e2ee_public_device_approvals`   | `accountId`, `epochId`, `deviceId`, `signerId`, `signature`, `recoveryRootId?`, `recoverySignature?`                                                                                      | challenge ID    | public     | Key-free signed approval: admits the device.                                                            |
| `__e2ee_account_successors`        | `accountId`, `predecessor`, `epochId`, `signerId`, `action`, `removedDeviceId?`, `retiredRecoveryRootIds`, `membership`, `revision`, `verification`, `history`, `deliveries`, `signature` | random          | private    | Private account rotation with the new key's deliveries.                                                 |
| `__e2ee_public_account_successors` | the private columns minus `verification`, `history`, `deliveries`                                                                                                                         | same as private | public     | Key-free signed rotation: the membership change.                                                        |
| `__e2ee_recovery_roots`            | `accountId`, `signerId`, `epochId`, `publicKey`, `mechanism`, `version`, `signingPublicKey`, `signingMechanism`, `signingVersion`, `signature`                                            | root ID         | public     | A registered recovery credential, signed by an active device.                                           |
| `__e2ee_recovery_deliveries`       | `rootId`, `epochId`, `envelope`                                                                                                                                                           | random          | private    | Account epoch key sealed to a recovery root.                                                            |
| `__e2ee_device_removal_requests`   | `accountId`, `epochId`, `deviceId`, `signature`                                                                                                                                           | random          | public     | A device's signed request to be removed; another active device completes it (§2.8).                     |
| `__e2ee_account_keys`              | `accountId`, `epochId`, `signerId`, `mechanism`, `version`, `publicKey`, `signature`                                                                                                      | random          | public     | An account epoch's sealing public key (§2.12), signed by an active device.                              |

`action` is `remove-device` or `retire-recovery-root`; the matching target
column is set. `retiredRecoveryRootIds` is an ID list (§0) of recovery root
IDs, versions 4, 7 or 8. A `retire-recovery-root` successor leaves
`removedDeviceId` unset and lists exactly one root, its target; a
`remove-device` successor sets `removedDeviceId` and lists every still
active root the removed device held the key pair of (§2.8), possibly none
(`[]`).

A root's `kind` is `device-account` (an account founded by a device-account
identity, §2.3) or `app` (an account of the application's own sign-in). A
`device-account` root sets both `deviceAccountPublicKey` (the raw 32-byte Ed25519
public key) and `deviceAccountSignature`; an `app` root sets neither. Any other
combination is malformed.

The identity ID is the account ID for generation 0 (the first bootstrap). For
a reset (§2.11) it is random (§0); clients find a generation's identity by
its account, generation, device and epoch (§2.4, "Private verification").

### 1.3 Space tables

These tables are internal too. One set serves every space table of the
application. Their policies are derived from the application's encryption
declarations (§1.4) by one function that the server runs at deploy time and
the app loaders run too, so both compute the same permissions bundle ([core
hooks](E2EE_CORE_HOOKS.md) H7); the server's result is authoritative, and
the application writes no policy for them. No application table may
reference a system table or inherit a policy through one; an application
policy may only test the members table with `exists` (R13). Every row carries the space's address, `scopeId` and
`identifier`, through which its policies reach the space row.

| Table                     | Columns                                                                                                                                                                              | Row ID | Meaning and writer                                                                                                                                      |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `__e2ee_members`          | `scopeId`, `identifier`, `accountId`                                                                                                                                                 | random | `accountId` is a member of the space. Inserted by an updater, or by the space creator for itself; deleted by an updater or by `accountId` itself (R12). |
| `__e2ee_spaces`           | `scopeId`, `identifier`, `creatorAccountId`, `deviceId`, `accountEpochId`, `epochId`, `mechanism`, `version`, `verification`, `accountEnvelope`, `signature`                         | random | A space root candidate: the first key version. Written by an updater (§3.2).                                                                            |
| `__e2ee_space_successors` | `scopeId`, `identifier`, `predecessor`, `historyEpochId`, `epochId`, `authorAccountId`, `authorDeviceId`, `authorEpochId`, `verification`, `history`, `accountEnvelope`, `signature` | random | A rotation: the next key version, linked to the previous one. Written by an updater (§3.5).                                                             |
| `__e2ee_space_deliveries` | `scopeId`, `identifier`, `keyEpochId`, `senderAccountId`, `recipientAccountId`, `recipientAccountEpochId`, `envelope`                                                                | random | A key version sealed to a member's account. Written by a member that holds it, to a member (§3.4).                                                      |

**The members table** is the one internal table the application uses
directly. It appears in generated client code; the application reads it (for
example to list a project's members) and inserts and deletes its rows
through the ordinary API, under the derived policies of R12. Its rows are
never updated. Membership is per account: there are no groups (groups of
accounts are possible later work). One row makes the account a member; the
row ID carries no meaning, and a second row for the same account and space
changes nothing, so removing a member deletes every row of that account for
the space. A trusted backend writes it like any application table, without
policies. The first release supports up to 1,000 member accounts per space
(§3.8).

The system tables define indexes on the columns their policies and the
runtime's queries look up: `(scopeId, identifier)` on every space table,
`(scopeId, identifier, accountId)` and `accountId` on members (the first
also serves lookups by space), `(scopeId, identifier, keyEpochId)` and
`recipientAccountId` on deliveries; on the account tables, `accountId` and
`deviceId` where present.

**Filling the space address.** `scopeId` is minted by the authority, so the
application never writes it: the generated members API takes the space
table's name and the space row's ID and fills `scopeId` from Jazz's table
identity ([core hooks](E2EE_CORE_HOOKS.md)
H1). Before the client knows that identity (a first-ever offline open), the
insert fails with the table-identity "not observed" error instead of
guessing. A trusted backend fills it the same way through its client.

**Membership is per account, per space.** Access by organisation, team or
role is not expressed in the members table: each account needs its own
member row in each space. An application whose access follows organisations
or roles should therefore choose a coarse space table (the organisation or
workspace row, with its records as rows that name it), not one space per
record, so that a membership change is one row and not one per record. One
space per record suits records shared with a few named people.

**After the space row is deleted.** Member rows and every key record of the
space remain (R1). Because `InheritsFrom` passes nothing for a deleted space
row, nobody can add members, create the space or rotate it; members may
still leave (R12), read the key records and deliver to each other (R14,
R17), so encrypted rows of the space that the application keeps stay
decryptable by its members wherever Jazz still lets them read those rows. This is intended: deleting the space row ends the
space's administration, not its members' access to content that remains.
The runtime makes no automatic rotation for a space whose
space row it sees deleted. An application that wants the members gone
deletes their member rows in the same transaction as the space row (a delete
is checked against the transaction's pre-state, R12). Restoring the space
row resumes administration.

Each encrypted application table stores every encrypted column as non-null
`bytes` (ciphertext; a null value is encrypted as a null value), plus a
non-null `bytes` column `__e2ee_eq_<column>` for each equality-indexed column.
Two short values are never ciphertext and have fixed meanings (§4.1): the
empty byte string (never written) and the 5-byte cleared marker
`4a 45 32 58 01` (written by a schema conversion, [migrations](E2EE_MIGRATIONS.md) §3.2).

### 1.4 Permissions

**The backend is not an encryption participant.** An app's backend (the
trusted process that relays its clients and runs its own code) has no account,
no identity and no keys in this protocol, and never will: every device and
every key belongs to a client. A backend starts for an app that declares
encryption and relays its clients as for any app; its own database opens with
keyless hooks built from the app's declarations. Through them encrypted
columns are never readable to it (a read returns the rows of an encrypted
table without those columns and their equality tokens, never stored bytes),
a write that names an encrypted column is refused ("the backend holds no
encryption keys; encrypted columns are written by clients"), and a query that
filters, orders or selects one is refused naming the pattern. Plain tables and
the plain columns of an encrypted table work as for any app, and member rows
are plain, so a trusted backend writes them like any application table
(§1.3).

Jazz is the only permission authority. E2EE adds no permission logic of its
own: replay authenticates signatures, device activity, chains and IDs, but it
does not decide who may read or update a space, and possession of a key
grants no Jazz permission. The server enforces access only, with policies;
it checks no signature, key chain, marker or envelope of any E2EE record,
account records included. Clients verify those on replay and ignore a row
that fails (§2.4, §3.3–§3.4). The policies below are what Jazz enforces on
the internal tables.

All E2EE tables:

- R1. No update by any writer, and no delete except of member rows (R12).
  Policies alone cannot say "ever": system and trusted-backend writes bypass
  policies, so core refuses these updates and deletes for every writer, as a
  system-table rule outside the policies ([core hooks](E2EE_CORE_HOOKS.md)
  H7).
- R2. Every row's author, sender or creator column (`accountId` of the
  account tables, `creatorAccountId`, `authorAccountId` and
  `senderAccountId`, as applicable) equals the session
  account and `$createdBy.account`. The
  `accountId` of `__e2ee_members` is not an author column: it names the
  member being added, which may be any account. Only R12's creator clause
  requires it to be the session account.

Account and device tables (fixed policies):

- R3. `__e2ee_device_requests`: insert by any account as `$createdBy`; read by
  creator only.
- R4. `__e2ee_device_keys`: insert only when the request whose row ID equals
  this row's `deviceId`, created by the same account, has identical key and
  mechanism columns.
- R5. `__e2ee_account_identities`: insert only with the row's `generation`
  (§1.2) and a request for `deviceId` created by the same account;
  generation 0 only with the session account as the row ID and when the
  account has no identity, a later generation only when the account has an
  identity of the generation before it, the session is not `device-account`,
  the account was not founded by a device-account identity, and the deployed
  encryption configuration sets `allowAccountReset` (§2.11); read by that
  account.
- R6. `__e2ee_account_roots`: insert only with `accountId = session account`
  and a matching identity (same device, epoch, generation and ledger
  version) by the same account, in the same transaction. The root's `kind`
  is `device-account` exactly when the account was founded by a device-account
  identity (Jazz's account registry); an account founded otherwise writes
  `app` roots.
  An encrypting app serves device accounts founded for its own app id
  only. A device-account identity's account ID derives from the audience its
  proof names (§2.3), which may be another than the app's, and the registry
  keeps no record of which one founded an account, so such an account could
  not be told from the app's own. The server therefore refuses to found one
  for an app that encrypts (`found-local-first`, and a guest's first
  admission), and accepts no identity or root (R5, R6) for an account whose
  identities are all device-account identities and none of which derives its ID in the
  app's namespace: neither a `device-account` nor an `app` root, so neither a
  bootstrap nor a reset. An account with an external identity is not told
  apart from one the app founded: a device account of another audience
  that was linked to an external login before the app encrypted counts as
  the app's, so that login can reset it (§2.11), like any account's own
  login.
  The rule also covers accounts founded before the app turned encryption
  on: a device account founded for another audience by then is locked
  out of encryption for good, and can neither bootstrap nor reset. There is
  no migration, and the registry records no namespace to move it by. This
  is intended. The deploy that first turns encryption on does not list
  such accounts: finding them reads every device-account identity in the
  registry, work that grows with the app's accounts, not with the deploy.
- R7. Challenges, proofs, approvals, device deliveries: insert by `$createdBy`
  only, referencing a request or challenge of the same account; read by
  creator.
- R8. Public device approvals: `accountId = session account`; the row ID is
  the ID of a private approval of the same account inserted in the same
  transaction; the signer's
  request and the approved device's request belong to that account; a
  recovery approval's `recoveryRootId` names a recovery root of that account.
  Both successor tables: `accountId = session account`; the signer request
  and the removed device's request belong to that account; the target
  columns match `action` (§1.2). The server does not check that a retired
  recovery root belongs to the account: the target is an ID in a list column,
  which no policy reads. Clients check it on replay (§2.4 step 3: the target
  must be a valid, earlier, unretired recovery root of the account's own
  ledger), so a successor retiring another account's root is not accepted. A
  public successor requires the private one with its ID, inserted in the same
  transaction. The reverse is not enforced: two rows cannot each require the
  other of one transaction, and a private approval or successor without its
  public half is inert (replay reads only public rows, and private
  verification drops a private row no public one pairs with). Removal requests: `accountId = session account`, and the
  request of `deviceId` belongs to that account.
- R9. Recovery roots: `accountId = session account`, signer request of the
  same account. Recovery deliveries: the root's `accountId` is the session
  account; read by creator.
- R10. Public tables (roots, device keys, public approvals, public successors,
  recovery roots, account keys, removal requests) readable by every `device-account` or `external`
  session, so other accounts can replay an account's history.
- R11. `__e2ee_account_keys`: insert only with `accountId = session account`
  and a signer request of the same account; readable like the other public
  tables (R10).

The founder, kind and `allowAccountReset` conditions of R5 and R6 are
account-registry and configuration checks beside the policies, as Jazz's
account registry already is; they decide who may reset or bootstrap,
not whether a record is authentic.

Space tables (policies derived at deploy). Each internal space row names a
space row by `(scopeId, identifier)`. Its policies reach that row through
`InheritsFrom` ([core hooks](E2EE_CORE_HOOKS.md) H7): `scopeId` must be the
catalogue identity of a space table declared at deploy, and the row of that
table whose ID is `identifier` must pass that table's own policy for the
operation. A row naming any other table, or a missing or deleted row,
passes nothing. Below:

- **read(row)** means: the space row, as the checking transaction sees it,
  passes the space table's read (`select`) policy for the session, exactly
  as a direct read of that row would, at any inheritance depth the direct
  read allows.
- **update(row)** means: the space row would pass an update by the session
  that changes nothing (a no-op save). Both clauses of the space table's
  update policy are evaluated, with the same row image as old and new row:
  its `USING` clause (the old row) **and** its `WITH CHECK` clause (the new
  row; `USING` again when the policy has no `WITH CHECK`, as for an ordinary
  update). This is not the single clause the existing `Inherits` node uses
  for an update. The row image is the space row as the
  checking transaction leaves it: its committed state plus the
  transaction's own writes once each has passed its own checks (Jazz's
  grounding rule, [authorization](7_authorization.md)), so a space row
  inserted or updated in the same transaction as the root is checked as
  written. No write is evidence for its own check (R12). A policy that requires an
  actual change of a column (an old-to-new transition) therefore admits no
  member change, creation or rotation; the deploy warns when it can see one.

Both are evaluated by the authority at the operation's commit, against its
state then. **member(a)** means: the members table has a row with the
checked row's `scopeId` and `identifier` and `accountId = a` (an `Exists`
over the members table, correlated with the checked row). **creator(row)**
means: the checking transaction itself inserts the space row, and the
row's `$createdBy.account` is the session account (an `Exists` over
the space table reached through the same `(scopeId, identifier)` link; it
reads the space row as raw evidence and does not apply the space table's
read policy).

- R12. **Members.** A member row is readable iff its `accountId` is the
  session account or member(session account): members see the space's
  other members. Insert iff read(row) and update(row), **or**
  `accountId` is the session account and creator(row) (the **creator
  clause**). Delete iff `accountId` is the session account (anyone may
  leave a space), or read(row) and update(row). A delete is checked against
  the state before the transaction's own writes (its pre-state), so several
  deletes in one transaction pass or fail independently of their order: an
  updater may delete its own member row and other members' rows together.
  Jazz's authorization rules already say this of `USING` clauses; the code
  does not yet do it for a delete whose policy reads the deleted row's own
  table ([core hooks](E2EE_CORE_HOOKS.md) H7, to be built). A row the
  transaction deletes still counts as evidence for its other writes (Jazz's
  existing rule, [authorization](7_authorization.md)), so a transaction that
  removes a member and also delivers to it, or an updater that removes itself
  and rotates in the same transaction, is accepted as if the removal came
  after. That is harmless: the removed account then holds the delivered
  version, which marks the space as needing rotation (§3.5), and the runtime
  never combines these writes. Leaving is
  a removal like any other: it deletes every member row of the account for
  the space (§1.3), and the leaver still holds the current key, so the space
  needs rotation (§3.5).

  **Why the creator clause.** A new space's first member cannot be admitted
  by read(row): read(row) needs a member row (R13), and Jazz never lets a
  write be the evidence for its own check. Within a transaction, a written
  row becomes evidence only after its own checks pass, and the row under
  check never counts for itself ([authorization](7_authorization.md),
  "Policy evidence inside a transaction"). So a space row and its first
  member row, in any order and in any kind of transaction, are refused if
  the member row can pass only through read(row). The creator clause does
  not read the members table: once the space row has passed its own insert
  check it is evidence, and the creator's member row passes on it.

  **Only in the creating transaction.** The creator clause holds only
  while the space row is being inserted: in a later transaction the row is
  no longer the checking transaction's own insert, and the clause is false.
  So a removed creator cannot add itself back, and switching encryption on
  for a table gives the creators of its existing rows nothing: those rows
  were inserted by earlier transactions. Only updaters, or a trusted
  backend, add members to existing rows. The clause depends on no clock. An
  application must insert the creator's member row in the same transaction
  as the space row. Core has no such check today; it gains one
  (`InsertedByThisTransaction`, [core hooks](E2EE_CORE_HOOKS.md) H7).

  Required tests: a fresh space row and its creator's member row in one
  transaction are accepted, in both insert orders, in mergeable,
  serializable and handle transactions (a handle transaction staged at the
  authority checks each write as it is staged, so there the space row is
  inserted first; both orders hold for a unit uploaded to it); the same
  transaction with the
  member row admissible only through read(row) and update(row) (the creator
  clause removed) is refused, pinning the grounding rule; a session that is
  neither the creator nor an updater adding itself to an existing space is
  refused; the creator adding another account by the creator clause alone
  is refused; the creator adding itself in a later transaction than the
  space row's insert is refused, also right after the insert and also
  after being removed; a member that may not update the space row
  deletes its own member rows, including when a further read conjunct
  (R13) hides the space row from it, and is accepted; such a member deleting
  another account's member row is refused; an updater deleting its own
  member row is accepted; the creator of a row that existed before its
  table became a space table adding itself is refused, and an updater
  adding it is accepted; a restored space row does not admit its creator.

- R13. **Space row read rule.** Every space table that any encrypted table
  with at least one encrypted column names as its `scope` has a read
  (`select`) policy that is `M(T)` or a conjunction that has `M(T)` as one of its conjuncts, where
  `T` is that table and `M(T)` is exactly
  `exists __e2ee_members(scopeId = table_id('T'), identifier = this.id,
accountId = session.user)` (`table_id` is T's catalogue identity, [core
  hooks](E2EE_CORE_HOOKS.md) H7). The deploy refuses any other read rule for
  a space table: a session that could read the space row without a member
  row would never receive a key. (A declaration without encrypted columns
  lets an existing app write member rows before it changes the read rule,
  [migrations](E2EE_MIGRATIONS.md) §4.1.) Further conjuncts (claims, row columns)
  narrow which members receive the rows, not the keys.
- R14. **Reads.** The roots, successors and deliveries of a space
  are readable iff member(session account). A delivery's envelope is sealed to
  its recipient account, so other members learn only that it was
  delivered, by whom, to whom and for which version.
- R15. **Create.** Insert into `__e2ee_spaces` iff member(session
  account), read(row) and update(row). The server does not limit the number of roots of an address:
  clients decide which is the space's root (§3.2).
- R16. **Rotate.** Insert into `__e2ee_space_successors` iff
  member(session account), read(row) and update(row). Once a space table's
  read rule comes from the members table (R13), read(row) already implies
  membership; the explicit condition also covers the first migration step,
  when it does not yet ([migrations](E2EE_MIGRATIONS.md) §4.1), so a
  session that is not a member never holds a space key it wrote itself.
- R17. **Deliver.** Insert into `__e2ee_space_deliveries` iff
  member(session account) and member(`recipientAccountId`).
- R18. Application tables keep their own read and write policies.
  Membership (R12, through `M(T)` in R13) is the cryptographic entitlement
  to everything in the space: every member receives the keys. Further
  conjuncts of the space row's read rule narrow which members Jazz sends the
  rows to, not which members can decrypt them (§1.5, §8).
  Nothing else is checked by the server: not that a root is the first of its
  address, that a successor's author holds the key, that a delivery names a
  version of its space, that an envelope opens,
  or how many members a space has (§3.8). Clients verify those, and
  ignore a row that fails (§3.2–§3.4).

### 1.5 App-file declarations

An encrypted table's `encryption` entry names its space: `space` is the
column holding the space row's ID (`id` for a table whose rows are their own
spaces), and `scope` is the space table. An encrypted column may declare a
plaintext `default` (§4.1), which is stored and deployed in plain text and
visible to the server, so it must not be secret; changing it later takes a
re-encrypt into a new column. Nothing else is declared in
`jazz.app.json`. In `jazz.permissions.json`, the space table's read rule
comes from the members table (R13), and its update rule decides who changes
members, creates and rotates:

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

```json
"projects": {
  "read": "exists __e2ee_members(scopeId = table_id('projects'), identifier = this.id, accountId = session.user)",
  "insert": "$createdBy.account = session.user",
  "update": "ownerId = session.user"
}
```

An application creating a project inserts the project row and its creator's
member row in one transaction. The project passes its own insert rule, which
must not need a member row of the new project (no member row can ground it,
R12). The member row then passes R12's creator clause on the project as
written; it cannot pass through read and update of the project, which need
the member row itself. If the same transaction also carries the space root
(§3.2), the root passes R15 once the member row has passed, so the project,
its first member and its root can be one transaction. A project inserted
without its creator's member row can get members only from a trusted
backend: an updater needs read(row), which needs a member row, so nobody
can read or manage it until then. Every other account is
added by an updater.

- **Column entries.** A `columns` value is either a type string (`"title":
"string"`) or, during a schema conversion, an object with `type` and the
  conversion fields of [migrations](E2EE_MIGRATIONS.md) §3.2 (`from`, `to`,
  `convert`, `sealedType`); the string is shorthand for `{ "type": … }`.
- **The system tables stay once added.** Once any version of an app carries
  the system tables (§1.2, §1.3), every later version carries them too, even
  one that declares no encryption (an app that stops encrypting keeps them,
  harmlessly: they hold the space keys and member rows its earlier versions
  wrote). A version without them that follows one with them is refused at
  deploy, in particular by a migration that removes a system table: bringing
  them back in a later version would store them as new tables, stranding the
  system rows of the earlier version (honest users would lose their keys,
  and a member removed under the later version could still be read as one by
  the earlier). The loaders append the fragment to every version after the
  first one that carries it, so both sides compute the same schema and hash;
  a later version without declarations gets the system tables' policies
  derived from the declarations of the nearest version before it that has
  them (the nearest wins, however far apart earlier declaring versions sit,
  and without a warning; two nearest versions that declare differently are
  refused). A deploy targeting a version that inherits the system tables
  must supply the declarations needed to derive their policies.
  The stored catalogue decides what came earlier: a new version
  without the system tables is refused when any stored version has them,
  whatever lens it hangs on and whether or not the deploy allows dropping
  history. Only a stored lens could show it to be earlier than a version
  that has them, and stored lenses join stored versions only; a lens the
  deploy brings (say, from the new version back to a stored one with the
  system tables) does not count. Once any stored version has the system
  tables, a deploy also refuses to target an older stored version without
  them, even when the deploy allows dropping history. Such a rollback would
  put application code from before encryption in front of encrypted data.
  Turning encryption off is a schema conversion of the columns (a decrypt,
  [migrations](E2EE_MIGRATIONS.md) §3), never a removal of the system tables.
- **Encrypted columns are last-writer-wins.** The server cannot merge
  ciphertext, so the deploy and code generation refuse a merge strategy
  (`counter`, `g-set`, `text`, `markdown`, `html`) on an encrypted column.
- The deploy and code generation refuse a `scope` that is not a table with an
  `id`, and a `space` column that is not `id` (with `scope` the table itself)
  or a `uuid` column referencing `scope`.
- **Entitlement is membership.** The deploy refuses a space table whose read
  rule does not come from the members table (R13). Finer rules inside a
  space (a private task in a shared project) are not cryptographically
  separated: every member of the project can decrypt the private task.
  Content that needs a narrower readership gets its own space:
  documentation recommends a dedicated content row (for example a
  `privateNotes` row per owner) with its own members. The deploy warns when
  it can see that the space row is readable more broadly than the encrypted
  table (for example, the table's read rule is the space row's rule plus
  further conditions), and when it can see that the encrypted table is
  readable more broadly than its space row (for example, the table's read
  rule does not involve the space row at all): a session that may read an
  encrypted row but is not a member receives the ciphertext and never a
  key, so it reports the row `key-unavailable`. It cannot decide either
  comparison in general and does not claim to.
- **Space row insert rule.** A space table's insert rule must be satisfiable
  without a member row of the row being inserted (for example
  `$createdBy.account = session.user`): such a member row cannot ground it
  (R12). The deploy warns when it can see an insert rule of a space table
  that reads `__e2ee_members` for the row being inserted.
- **Space row update rule = managing members.** Whoever may update a space
  row may add any account to the space, remove any member, create the space
  and rotate its key (R12, R15, R16). In the example above only the
  project's owner manages members. A rule such as "any member may rename the
  project" would let every member add anyone. Fields that members other than
  the managers may change belong in another table (for example a
  `projectSettings` row that names the project), whose update rule can be
  broader. The deploy warns when it can see that a space table's update
  rule is broad: when an update clause is `true`, when it is exactly the
  members read rule `M(T)`, or when it reads no session value
  (no `session.*` and no `$createdBy`), since then every member, or every
  reader, manages members. It cannot decide breadth in general and does not
  claim to.

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
if absent. The request carries `nonceCommitment = SHA-256(UTF8(
"jazz.e2ee.approval-nonce.v1\0") ‖ frame[deviceId, n_R])` of the device's
random 32-byte **approval nonce** `n_R`, which the device keeps until its
approval (§2.5). A device answers at most one challenge with its nonce, in an
approval or a recovery: it refuses any further challenge
(`invalid-challenge`), and keeps that state with the device. A device whose
attempt failed or was cancelled after that starts again as a new local
device (§2.1), with a new ID and request. An existing row whose keys differ refuses ("immutable device
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
kind, deviceAccountPublicKey?, deviceAccountSignature?}`,
- the account sealing key record of `epochId` (§2.12),

where `A(recipient, column) = ctx{application: D, policy:
"jazz.e2ee.account-identity.v1", scope: "account", identifier: account,
table: "__e2ee_account_identities", row: account, column, epoch: epochId,
recipient}`. Both envelopes are opened again before submission. At most one
bootstrap is accepted (§0 item 5).

**How a root is authenticated depends on how the account signs in.**

- **Device accounts** (`packages/jazz-tools/src/local-first-auth`). The
  account secret derives an Ed25519 key (`SHA-512(UTF8("jazz-auth-sign-v1")
  ‖ secret)[0..32]` as the seed), whose public key `pk` derives the
  **subject** `UUIDv5(namespace = UTF8("jazz-auth-key-v1"), name = pk)` (the
  16 ASCII bytes are the namespace), which Jazz's account registry turns into
  the account ID `UUIDv5(namespace = appId, name =
UTF8("jazz-account-founder-v1") ‖ 0x00 ‖ UTF8(subject))`. The root has
  `kind: "device-account"`, `deviceAccountPublicKey: pk`, and `deviceAccountSignature`,
  a `jazz.sodium.sign` v1 signature by that key over

  `ctx{application: D, policy: "jazz.e2ee.device-account-root.v1",
scope: "account", identifier: account, table: "__e2ee_account_roots", row:
rootId, column: "device-account-signature", epoch: epochId, recipient:
deviceId} ‖ frame[decimal generation, decimal ledgerVersion, pk,
env(signingMechanism, signingPublicKey), env(mechanism, publicKey)]`

  with the root device's published key columns: the signature binds
  everything the root introduces (account, generation, epoch, device and its
  keys). Writing it needs the account secret; the SDK holds the derived key
  pair only in memory, never prints it, and refuses
  (`device-account-identity-mismatch`) an identity whose key does not derive
  the account ID. Nobody without the secret, the server included, can write
  or substitute such a root that clients accept: the server stores any root
  the account's sessions write (R6), and replay rejects one whose key or
  signature does not check (§2.4 step 1).
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
   - it is `device-account` and its public key does not derive the account ID
     (`root-key-mismatch`), its device has no published keys accepted at or
     before the root (`root-device-keys-missing`), or its signature over the
     bytes of §2.3, with those keys, does not verify
     (`root-signature-invalid`). Anyone can copy an account's public key into
     a row naming a device without keys; such a row is rejected like any
     other forgery and never stops replay;
   - it is `app` and the read has a valid `device-account` root of the account
     (`root-kind-mismatch`): a device account's roots of every
     generation are signed by its identity, so its kind cannot be
     downgraded, and replay accepts a later generation of it only if the
     account's device-account key signed it (§2.11).

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
   (§2.5), which cover the recipient's published keys, verifies, and it is
   not a non-recovery approval carrying a `recoverySignature`. A recovery
   approval (§2.9) is accepted regardless of the signer's membership. A
   false verification skips the row; a signer failure propagates. An
   approval whose recipient has no published keys cannot be verified and is
   skipped, like a false verification. A keys row other than the one the
   signer approved (a substitution) therefore leaves the device inactive:
   only keys some approval or a signed root vouches for are ever used to
   seal to a device or to verify its signature. The one exception is the
   root device of an `app` account, which nothing signs (§2.3); the floor
   records its keys (below).
3. **Successors.** Visited one position at a time, in position order. A
   candidate is **valid** unless its account or predecessor differs, it is
   not strictly after the current epoch position, its epoch was already
   used, it is malformed, its public `revision` differs from the recomputed
   one, its signer was not active before it, its target is not an active
   device other than its signer (removal) or not a valid, earlier,
   unretired recovery root (retirement), its signature fails, or its `membership` is not exactly the
   active set minus the target. Rows at one position were written in one
   transaction, which only the account itself or the server can produce;
   their order is undefined. So a valid candidate is accepted only if it is
   the only valid successor of the current epoch at its position and no
   valid recovery root (§2.9) is at that position; otherwise none of them
   is. Rows that fail any check, successors or recovery roots, never take
   part in this rule, so an invalid row cannot block a valid successor. The
   accepted successor advances the epoch.
4. Unsupported signing mechanisms or versions raise an
   unsupported-capability error (§7); they are never treated as invalid. The
   same holds in private verification (below): a private record whose
   signer's request names an unsupported signing mechanism stops it with
   that error; it is not skipped.

The result is the current account epoch, its active devices, removed devices,
active and retired recovery roots, and the active devices with a pending
removal request (§2.8).

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

- a device holding the device-account identity: the canonical root is not a
  `device-account` root of its key;
- private verification (below): no identity of the account of the canonical
  generation matches the canonical root (same device and epoch, at its
  position);
- a floor (below) whose root is not the canonical root of its generation in
  the chain.

For an app account the floor is the only check that catches a substitution
the server makes after the device first verified the account; before that
first observation the device trusts what it sees (§8).

**Floors.** After it verifies an account state, a device persists, in its
private store, the account's **floor**: the read's cut, the generation, the
canonical root of that generation with its device's keys, and the current
epoch with its position.
A later read is refused with `stale-state` if its cut is earlier, its
generation lower, or (same generation) the floor's epoch is not in its
chain; it is `root-not-mine` if the chain's root of the floor's generation is
not the floor's root, or its device's keys are not the floor's. A floor of the device's own account also records
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
advances to the newer state (§6.3a).

**Private verification.** A device that wants the account key also reads the
private tables (same cut) and accepts: the identity only if it matches the
public root (same generation, device, epoch and ledger version) at exactly
the same position, and, for generation 0, its ID is the account ID; a
private approval only if its
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
   kept only in memory, and a random 32-byte nonce `n_A`. Insert challenge
   `{id, deviceId, epochId: current, envelope: E = seal(requester.pub,
P("challenge"), S), nonce: n_A, signerId, signature:
sign(P("challenge-signature:" ‖ signerId) ‖ frame[env(signingMechanism,
signingPublicKey), env(mechanism, publicKey), nonceCommitment, E, n_A])}`,
   signed over the requester's key columns and nonce commitment as the
   approver was shown them. A challenge is created only on an explicit user
   action, one per approval: the runtime never re-challenges silently.
2. **Response** (stage `proof`, requesting device). The challenge targets this
   device in the current epoch, and its `signerId` is an active device whose
   signature verifies over this device's own key columns and nonce
   commitment; otherwise the requester refuses it (`invalid-challenge`): the
   approver was shown other keys. Open `S` (32 bytes). Insert proof `{id:
challengeId, proof: wrap(S, P("proof"), Z32), nonce: n_R, signature:
sign(P("proof-signature") ‖ frame[proof, n_R])}`, revealing the approval
   nonce.
3. **Completion** (stage `approval`, approving device). The challenge epoch
   equals both the challenge's and the current epoch, and the request is the
   one the challenge was signed over (`stale-challenge` otherwise: a request
   read again at completion may have been swapped). Possession is verified:
   the proof's `nonce` opens the request's `nonceCommitment`,
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
"approval-signature:" ‖ signerId, epoch, recipient: deviceId} ‖
frame[env(signingMechanism, signingPublicKey), env(mechanism, publicKey)]`,
     with the requester's key columns from its request. The approval thereby
     vouches for the device's keys, not only for its ID: the keys row (R4) is
     unsigned, and only the server ties it to the request;
   - delivery `{id: challengeId, envelope: E = seal(requester.pub,
P("delivery"), K), verification: wrap(K, P("delivery-verification") ‖
frame[E], Z32)}`.

   Every marker is unwrapped again and every signature verified before
   submission. The secret is erased when the operation ends.

**Approval code.** The keys row of a requesting device is unsigned (R4), so
the approver sees the requester's keys only as the server shows them; and a
new device of an `app` account knows the account's devices only as the
server shows them (§8). Both devices therefore display a six-digit
**approval code**: the first four bytes (big-endian) of
`SHA-256(P("approval-code") ‖ frame[env(signingMechanism, signingPublicKey),
env(mechanism, publicKey), n_A, n_R])` modulo 10⁶, zero-padded, with the
requester's key columns. The requester computes it from its own keys, the
challenge's `n_A` and its own `n_R` once it has accepted the challenge (and
shows an error, never a code, if it refused one); the approver from the keys
it was shown, its `n_A` and the revealed `n_R` once possession is verified,
and only for the challenge it completes. The SDK asks the user to confirm
the two codes match before the approval is inserted; a mismatch means
someone between the devices substituted keys, and the user cancels.

The code is a short authentication string with commitments: each side fixes
its nonce before it learns the other's (the requester commits to `n_R` in
its request, before any challenge exists; the approver signs `n_A` in the
challenge, before `n_R` is revealed). Whoever sits between the devices, the
server included, must therefore choose what it shows each side before
learning the value that decides the other side's code, and matches the codes
only by a one in a million chance, even when it plays the account's devices
towards a new device it meets first. An approval made without comparing the
codes trusts the server's view of the requester's keys (§8). Private
verification (§2.4) accepts an approval only if its challenge's signature
verifies under the approval signer's key over the requester's keys and
nonce commitment, and the proof's nonce opens that commitment. A recovery
(§2.9) signs its own challenge, the device being both approver and
requester; its recovery root is what authorises it.

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
acquisition (§2.9) and the possession check (§2.5) follow the same rule.
Account key acquisition skips a candidate of an unregistered mechanism or
version and reports `account-key-envelope` if it skipped such a candidate
and none yields an authenticated key.

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
  proof is ignored **only** when provably invalid (malformed or failed
  authentication, or a marker not `Z32`). Ignoring it does not revoke the device or change
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

Stage `account-rotation`; action `remove-device` (target: an active device
other than the author) or `retire-recovery-root` (target: an active recovery
root). A device never removes itself (`self-removal`): the author generates
`K'`, so a device removing itself would keep the new epoch's key, and every
space key later sealed to the account at that epoch. Another active device
removes it: a device that wants to leave writes a removal request (below).
The author is active
and holds the current key `K`, authenticated (§2.6) against a state with
exactly the public approvals of the current epoch that the rotation's state
has: a key authenticated at an earlier read, before an approval that has
since been accepted, is re-authenticated first (the rotation refuses it
until then), since a private `revision` without that approval could not be
verified by any device. A fresh epoch key `K'` and epoch ID are always
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
("Retiring a derived root"). A device account's removals list none
(§2.9: its root derives from the account secret every device holds).
Otherwise removal does not retire recovery roots, and retiring a root does
not remove devices. Only one rotation from a given predecessor can win.

**What removal achieves.** Removal withholds **future** keys only: the
account's next epoch keys, and so every space version sealed to the account
from then on. It takes nothing back. The removed device keeps every account
key and every space version it held, and space keys are not rotated by an
account rotation; while its sign-in to the application stays valid, Jazz
keeps sending it the account's rows, and rows written under versions it holds
stay readable to it. To cut a lost or stolen device off, the account must
first **revoke the device's sign-in** with the application (so Jazz stops
serving it); its spaces then need new keys. A device removal marks every
space whose current version was sealed to an account epoch the removed
device held as needing rotation (§3.5), and the next encrypted write by an
updater rotates it, as after a member's removal. An application that
wants it sooner calls `db.encryption.rotate(space)`. Developer documentation must
say to revoke the sign-in first.

**What removal achieves also depends on how the account signs in.** For an
account authenticated by the application's own sign-in, a removed device can
no longer obtain new keys: it is not active, receives no delivery of the new
account key and cannot approve itself. Recovery keys and the recovery secret
are never persisted on a device (§2.9), and a device that used them had its
root retired by its removal (above), so whatever it kept does not open the
new key. For a **device account**, whoever holds
the account secret is the account: every device of the account stores the
secret in order to sign in, and the recovery root is derived from it (§2.9).
A removed device of a device account therefore still signs in to Jazz as the account
(and keeps receiving the account's rows), derives the recovery key pair,
opens the recovery delivery of the new account key and approves a fresh
device ID with it. Removing a device does not protect against a device that
holds the account secret; the remedy for a lost or stolen device of a device account
is to move to a new account. The SDK says so when removing a device of a
device account. Removal still serves its other purposes there (retiring
an unused install, rotating away a device whose secret was never stored, such
as a device that only held a link).

**Removing the device you are on.** A device leaves its account by writing a
**removal request** `{id, accountId, epochId: current, deviceId: itself,
signature}`, signed by itself over `ctx{application: D, policy:
"jazz.e2ee.device-removal-request.v1", scope: "account", identifier:
account, table: "__e2ee_device_removal_requests", row: id, column:
"signature", epoch: epochId, recipient: deviceId}` (stage
`account-rotation`). It is refused with `no-other-device` unless the current
epoch has another active device: a recovery root can restore access but
cannot complete the removal, so the account's only device cannot leave this
way. The device stays an active member, and keeps its keys, until it is
removed; nothing else about it changes. The SDK shows it as "removal
pending, waiting for another device", never as removed. A request does not
outlive a reset: after one, a device approved again under the same ID is
not removed by its earlier request.

A request is **pending** while it is valid (its device was active with
`epochId` at the request's position, `epochId` is of the account's current
generation, and its signature verifies) and its
device is still active; a request that cannot be verified, including one
with a signing mechanism the client lacks, is not pending. Any other active
device that holds the current key and sees a pending request completes it,
automatically and once, with the ordinary `remove-device` rotation of that
device above, so the requesting device never holds the new epoch's key. When
the requesting device then sees that it is no longer active
(`device-no-longer-active`, §2.4 "Floors"), it wipes its E2EE state for the
account (§6.4); it wipes nothing before, so a user never loses keys before
another device holds the account. There is no cancellation: a device that
wants to stay after its request is removed and enrols again (§2.1).

Required tests: a pending request is completed by another device, after
which the requesting device is not active, cannot acquire the new key and
fails its floor with `device-no-longer-active`; the only device's request is
refused with `no-other-device`; a request with a bad signature, signed by
another device, or naming an epoch its device was not active in is not
pending, and does not stop a valid request from being pending.

### 2.9 Recovery

Recovery builds on the account backup that device accounts already have.
Device accounts (`packages/jazz-tools/src/local-first-auth`) keep a
32-byte **account secret**, `jazz-auth-v1:` followed by 43 unpadded base64url
characters (`secret.ts`, `formatSecret`/`parseSecret`). It derives the
account's signing key and subject, so whoever holds it is the account
(README, "Threat model"). It is backed up either as a 24-word BIP39 recovery
phrase (`recovery-phrase.ts`) or in a platform passkey whose user handle is
the 32 secret bytes, created and read with user verification required
(`passkey.ts`, `BrowserPasskeyBackup`, exported as `jazz-tools/passkey-backup`).

- **Recovery secret.** 32 bytes `s` from which the account's recovery root is
  derived:
  - **Device accounts:** `s` is the account secret's 32 bytes. Restoring
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
  with the version and variant bits of §0 ("Identifiers"). The labels
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
  device held its key pair; a device account's removals retire nothing.
- **Never persisted.** A device MUST NOT persist the recovery secret, the
  recovery seed, the recovery key pairs or exported material: they exist in
  memory for one registration or restore and are erased when it ends (a
  device account's secret is stored for sign-in by local-first auth, not
  by E2EE). Removing a device that registered a recovery root, or restored
  with one (with any of its recovery approvals), retires that root (§2.8).
- **Restore.** The device first obtains the recovery key pair: for a
  device account, by restoring the account secret (passkey or recovery
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
  approval found key lineage.
  Recovery skips a candidate of an unregistered mechanism or version and
  reports `account-key-envelope` if it skipped such a candidate and none
  yields an authenticated key.
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
  - **Device accounts:** the account secret is the account, and it cannot
    be rotated without becoming a different account. Retiring its derived root
    therefore only turns off recovery by account secret for this account (the
    account may still register exported material); it does not protect against
    a leaked account secret, because whoever holds the secret signs in as the
    account. A leaked device-account secret loses the account, E2EE
    included, and so does a lost or stolen device of a device account,
    which holds the secret (§2.8). Recovery stays derived from the account
    secret: there is no separate recovery secret and no device check on
    writes for device accounts. The SDK asks for confirmation before
    retiring a derived root of a device account.

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
deploy request). A device account has no reset without its secret:
there the account secret is the account, and losing it is losing the
account. The SDK never offers a reset for one; a holder of the secret
restores access by recovery (§2.9) instead, which needs no new generation.
The server enforces both: R5 refuses an identity of generation above 0 from
a device-account session, for an account founded by a device-account identity
(also when it signs in through a linked identity of the application), or
when the deployed configuration does not allow reset. Clients enforce the
device-account part themselves: replay accepts a later generation of a
device account only if the account's device-account key signed its root
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

**A reset account gets keys again from members.** Its new generation holds
no account key of the old one, so it opens nothing sealed to its old account
(§2.12). Holders see a member with no envelope for its current generation
and deliver to its new sealing key (§3.4). There is no approval: its member
rows decide, as for every member (§8). Members see the reset: each member's
runtime reads the public roots (R10) of the space's members and, for a
member whose current generation is above 0, emits an `account-reset`
encryption event `{account, space, generation}` once per space, account and
generation, next to the unreadable-row reports (§4.3), so the application
can show "<account>'s encryption was reset". An account that reset before
it joined the space is shown the same way. Member devices watch the public
roots of the space's members (§3.8), so a reset is seen, and redelivered
to, even when no space table changes.

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

A record is **valid** iff its signature verifies under its signer's
published signing key (accepted before the record's position, §2.4) and it
is one of two kinds:

- **Co-published.** The record is accepted at the same position as the
  accepted creation of its epoch, and its signer is that creation's
  authorised signer: for a root (bootstrap §2.3 or reset §2.11), the
  canonical root of its generation, whose `deviceId` must be the signer; for
  an account successor (§2.8), the accepted successor whose `epochId` is the
  record's, whose `signerId` must be the signer. The signer is authorised by
  the creation itself, not by membership of the new epoch: the root's device
  is admitted by the root, and a successor's signer was active before the
  successor (§2.4 step 3) even when it is the device the successor removes,
  which is then absent from the new membership. A record at that position
  naming any other signer, whether a device active in the predecessor epoch,
  a device the new membership admits, or a device of no standing, is not
  valid, and no later approval makes it valid.
- **Later publication.** The record is accepted strictly after its epoch's
  creation, its epoch is the account's current epoch as of its position, and
  its signer was active with that epoch at its position (§2.4, strictly
  before). This is the case of a mechanism migration (§0).

A record for an epoch that is not in the account's chain, or accepted before
its epoch's creation, is not valid. A sender seals to the first valid record (position, then ID) of
the account's current epoch and the selected mechanism; with none, sealing
fails with "recipient unsupported" and the sender waits. A device of the
account also checks that the record derives from `K`, for each mechanism it
supports. One that does not is reported (`conflicting-account-key`, §7) and
ignored by that device; it never stops the device acquiring its account
key. Only an active device of the account can sign a valid record; a row
the server writes without that is not valid and is ignored. Senders cannot
check derivation, so until the record is replaced they may seal to it. The
account's devices know which of their records do not derive, so a delivery
sealed to one is reported against that record and its signing device
(`conflicting-account-key`), never as `invalid-delivery` against the
sender (§3.4). The remedy is to remove
the device that signed it: the removal starts a new account epoch, whose
co-published record is correct. A record whose signer has no signing key
published before it is not valid. A record whose signer's signing mechanism
this client does not support is skipped and reported
(`unsupported-account-key`); it never fails the account's replay.

Required behavioural tests, each a separate case: a record co-published by
a bootstrap, by a reset and by a rotation is valid; a record co-published by
a rotation whose signer removes itself is valid; a record at a creation's
position signed by another device (one active in the predecessor epoch, one
admitted by the new membership, and one with no standing) is not; a later
publication by an active device is valid, and one by a device that was not
active, or for an epoch that is no longer current, is not; a valid later
record that does not derive from the account key is reported and ignored,
the account's devices still acquire the account key, and after the signing
device is removed the new epoch's record is used; a record signed with an
unsupported signing mechanism is skipped and reported, and the account's
replay succeeds.

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
epoch (§2.12). `K'` is the new version's key, `Kₕ` the key of the
successor's `historyEpochId` (its predecessor's key, except in a recovery
rotation, §3.5), and in a delivery `K` is the key of `keyEpochId`.

| Record    | Signed bytes                                                                                                                                                                            | Key material                                                                                                                                                                                                                                                                                                               |
| --------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Root      | `frame[X(json(["root", creatorAccountId, deviceId, accountEpochId]), "", epochId), env(mechanism, verification), accountEnvelope]`                                                      | `verification = wrap(K₀, X("verification", "", epochId), Z32)`; `accountEnvelope = seal(A(creatorAccountId, accountEpochId), X("account-envelope", json([creatorAccountId, accountEpochId]), epochId), K₀)`                                                                                                                |
| Successor | `X(json(["successor", id, "signature"]), json([authorAccountId, authorDeviceId, authorEpochId]), epochId) ‖ frame[predecessor, historyEpochId, verification, history, accountEnvelope]` | `verification = wrap(K', X("verification", "", epochId), Z32)`; `history = wrap(K', X(json(["successor", id, "history"]), historyEpochId, epochId), Kₕ)`; `accountEnvelope = seal(A(authorAccountId, authorEpochId), X(json(["successor", id, "account-envelope"]), json([authorAccountId, authorEpochId]), epochId), K')` |
| Delivery  | Unsigned; random ID                                                                                                                                                                     | `envelope = seal(A(recipientAccountId, recipientAccountEpochId), X(json(["delivery", id, senderAccountId]), json([recipientAccountId, recipientAccountEpochId]), keyEpochId), K)`                                                                                                                                          |

In the root, `env(mechanism, verification)` is applied literally: the root's
`mechanism` and `version` columns name the key wrapping mechanism of the
marker, and the signed field is the common envelope of that mechanism whose
payload is the whole `verification` column, which is itself already that
mechanism's envelope (`JE2E 01 ‖ len ‖ id ‖ u32be(version) ‖ verification`).

The successor's `historyEpochId` is signed and bound into the `history`
context (as its recipient field), so the version a `history` wrap opens
cannot be substituted.
Deliveries are unsigned: the server authenticates their
sender (`$createdBy`, R2), and a delivered key is accepted only after it
authenticates against its version's marker and the chain (§3.3), which the
signed root and successors carry.

### 3.2 Creation

A space exists once a valid root of its address is accepted. Only an
updater creates one (R15: read and update permission on the space row). The
creator generates the root epoch and `K₀`, and writes the root with
`accountEnvelope`, its own account's copy. The other members receive `K₀`
by delivery (§3.4).

**When a space is created.** Inserting a space row creates nothing. A space
is created only:

- **with the first encrypted write that needs it.** An encrypted write (or a
  conversion, [migrations](E2EE_MIGRATIONS.md) §3.4) to a space for which
  the client knows no valid root stages the root in the **same
  transaction** as the write ([core hooks](E2EE_CORE_HOOKS.md) H2), so the
  encrypted rows and their space are accepted or refused together. Online,
  the runtime first reads the address's roots, so it does not stage a root
  for a space that already has one;
- **explicitly**, by `db.encryption.createSpace(row)`: one transaction
  holding only the root (stage `create`), which waits for Global
  acceptance. Online it first reads the address's roots and returns the
  existing space if one has a valid root. It always attempts otherwise,
  even after an earlier refusal on this device (below), except while this device has a creation of the
  space whose outcome is pending or unknown: it then reconciles that
  creation and returns its outcome instead of staging a second root;
- by a conversion sweep, in its first conversion transaction for the space
  ([migrations](E2EE_MIGRATIONS.md) §3.5), unless an earlier attempt was
  refused (below).

**Creation is optimistic.** A Jazz replica evaluates no write policy, and
the runtime does not try to predict the authority's decision: it stages a
root whenever it needs a space that has none, online or offline, and the
authority decides (R15). Before a root is staged, the runtime prepares it:
online it reads the address's roots, and it generates `K₀`, writes the root
and stores `K₀` as an `unpublished` held key (§5.4). The write then stages
the prepared root. Inside a transaction callback the staging call awaits
this like an acquisition (§5.6). If a root was missed (offline, or not yet
synced), the new one is a lost creation (below). A member that may not
update the space row cannot create it.

**Outcomes of a creation attempt.** The runtime tells four outcomes apart:

- **Creation refused**: the authority refused the root row itself (R15).
  The SDK reports a publication error of stage `create` with code
  `space-create-refused`, naming the space, and the device stops
  automatic attempts for the space (below). When the transaction holds
  only the root (`createSpace`), a policy refusal
  of it is this outcome; a conflict or another failure of it is the next.
- **Transaction failed otherwise**: the transaction that carried the root
  failed definitively for another reason: another of its rows was refused,
  it conflicted, or a refusal does not name the root. The transaction's own
  error is reported, and automatic attempts continue: the failure says
  nothing about this device's right to create. The next write that needs
  the space prepares a fresh root.
- **Cancelled before submission**: nothing reached the authority. Only the
  cancellation is reported, and automatic attempts continue.
- **Unknown**: the outcome is reconciled by the transaction's durable
  identity (§5.4, §5.6), never by staging another root. While it is pending
  or unknown, no path, `createSpace` included, stages a second root for the
  space on this device.

Whatever the outcome, a key whose root did not become the space's root
follows §5.4 and is repaired as below.

**After a refusal.** Once a creation is refused, the runtime's automatic
paths (the first encrypted write and a conversion sweep) stage no root for that space on this device while the
client runs: encrypted writes to the space fail with `space-pending`, and
reads report it `space-pending` (§4.3), until a valid root exists. An
explicit `createSpace(row)` always attempts. This is kept only in the
memory of the running client; nothing about it is stored, so after a
restart the next automatic path may try the space once more. A space is
created only by someone who may update its row, through an encrypted write,
a conversion or `createSpace`; a member who may not update the row reports
the space `space-pending` until then. A session that is not a member is
refused (R15) and reports `space-create-refused`; it would get no key
anyway. Rotation follows the
same rule (§3.5).

Required tests: after a creation refused, an encrypted write fails
`space-pending` without staging a root, and an explicit `createSpace`
attempts and, once the session may update the space row, creates the
space; after a restart, at most one automatic attempt per space is made
before automatic attempts stop again; a transaction that fails because
another of its rows is refused, and a cancellation before submission, do
not stop automatic attempts; a `createSpace` while an earlier creation's
outcome is unknown reconciles it and stages no second root.

**Several roots.** The server does not limit roots per address (R15).
Concurrent creators, offline creators, and a creator whose root clients
reject (for example one written by a device that was removed before the
root was accepted) can all produce roots. Replay accepts a root iff its
creator had an account root strictly before it, its device was active with
account epoch `accountEpochId` at its position (§2.4), and its signature
verifies. The space's root is the **first valid root** of its address
(position, then ID); every other root is ignored. Root validity depends
only on account state as of the root's position, so every client picks the
same root, and an invalid first root never blocks a valid later one. A root
that is not the space's root is a **lost creation**.

**Refused and lost creations.** A creation whose root does not become the
space's root (refused, failed, cancelled before possible submission, or
accepted but lost) leaves this device with a key no one else will use. The
transaction that carried a root that was not accepted wrote nothing. Later
transactions that encrypted under its key, which do not depend on the
creation, may have been accepted by Jazz; their cells name an epoch that is
not in the space's chain, so every other member reports them
`invalid-ciphertext`. The device keeps the key for good, marked `refused`
(§6.2): it is never used for a new write, and always opens the cells this
device wrote under it. It repairs its own rows:

- if its account is a member, the runtime obtains the space's real key (by
  delivery, §3.4, once a valid root exists), then repairs the rows it wrote
  under the losing key (below);
- if it is not a member, or the session can no longer read the space row,
  those rows stay unreadable to everyone else.

**Repair** is an online background task of the runtime, built like the
conversion sweep ([migrations](E2EE_MIGRATIONS.md) §3.5). It finds the
space's rows with a cell naming the losing epoch (it reads the space's rows,
a cost proportional to the space), and rewrites them in serializable
transactions of bounded size (default 100 rows). Each transaction reads
every row it repairs, including the row's space coordinate and the exact
cells it rewrites; it decrypts the cells that still name the losing epoch,
and only those, and writes the same plaintext re-encrypted under the newest
held version of the space's chain (§5.2), with fresh equality tags. Other
cells are not written. Read-set validation (§0 item 5) refuses the
transaction if any row it read changed after its cut, so a concurrent edit,
move or delete is never overwritten: the runtime re-reads and repairs only
the cells that still name the losing epoch. A cell a later write replaced is
left as it is. Repair never replays an application callback or source; its
transactions are runtime publications, retried internally after a conflict
(§7). It needs Jazz update permission on the rows: a row whose repair is
refused is skipped and stays unreadable to others. It does not run offline.

**When repair runs.** A pass runs at the natural moments of the delivery
pass (§3.4: start, reconnect, the app's call, a watched space's change), once
the real key is held, and on the app's call for one space or for every space
with a refused key. It finds the rows in the device's replica, which includes
its own committed writes not yet accepted, so those are repaired like any
other row once the authority has accepted them (a read at one cut has no
position for a row it has not accepted: the pass waits and tries again). Since
the key is never deleted, nothing has to decide when repair is finished: a
write still open or of unknown outcome is repaired by a later pass.

The rewrite is an ordinary update of the cell's plaintext, sealed by the write
path under the newest key of the chain's root (§5.2) with a fresh equality
token: the cells that open under a refused creation's key are the ones the
batch's own read reports, so a cell a concurrent writer has replaced is not
among them. A streamed value under the losing key is rewritten as an ordinary
cell, in memory: repairing a value of n bytes takes at least n bytes of memory,
unlike the bounded memory of a streamed write (§5.5).

Since the rewrite is an ordinary update, Jazz permissions decide it like any
other: a creator that may not update the rows it wrote has the repair refused
by the authority (`authorization-denied`), and those rows stay unreadable to
the other members; the application's own rules decide whether that can happen.
The natural moments then stop repairing that space for the rest of the run,
like the in-run refusal of a creation (this section), and an explicit call
attempts again.

Required tests: device A wrote row R under the losing key with value `old`;
after the space's root is known, another member writes `new` to R under the
real key; A's repair, which read `old`, conflicts, re-reads and leaves
`new` untouched; a repair interrupted between batches keeps the key and
finishes after a restart; a row whose repair is refused stays unreadable to
others and readable to its author; a handle transaction that sealed a cell
under the losing key commits after a complete pass, and a later pass
repairs the row.

### 3.3 Key chain and acquiring a key

**Chain rule.** From the root's version, the next version is the first
successor of the address, in position order then ID, that names the current
version as `predecessor`, is strictly later than it, has an epoch not used
before, is well formed, whose `historyEpochId` is the predecessor or an
earlier version of the chain, whose author device was active with account
epoch `authorEpochId` at its position (§2.4), and whose signature verifies.
The last version reached is the **current version**. Whether the author
could update the space row is Jazz's decision and is not re-checked. Every
member applies the same rule.

**Acquiring.** The client reads, at one cut, the root, the successors, the
deliveries addressed to its account (R14) and the public account histories
of the authors involved. The candidates for a version `v` are the
`accountEnvelope` of `v`'s root or successor when its own account wrote it,
then deliveries of `v` to its account. Each names an epoch of its account;
the device opens it with that epoch's sealing key, derived from the account
epoch key it reaches through its account's chain (§2.6, §2.7, §2.12). A
candidate naming an epoch it cannot reach (another generation) is skipped,
as is a delivery at or before the root's position. An opened key is accepted
only if `v`'s marker verifies and each `history` unwraps, back to the root, a
key whose marker verifies; every earlier version's key that the history
links reach is thereby obtained too. A successor's `history` opens the key
of its `historyEpochId`; versions strictly between that version and the
successor were skipped by a recovery rotation (§3.5) and get no key from
the chain (precisely: a version is skipped when no history link from the
current version reaches it): a device obtains them only from its own account envelope or a
delivery of that version. A candidate that provably fails to open or
authenticate is skipped; a crypto provider failure fails the operation
(§0). The client tries the current version first and then earlier ones, and
keeps every key it accepts as a held key. When it does not obtain the
current version, it waits for a delivery (§3.4). A candidate sealed with a
mechanism this client does not support is skipped and reported as
unsupported, never as invalid.

**Unresolved history.** A record needing a capability this client lacks
(unsupported signing mechanism or ledger version) leaves the chain
unresolved from there (§7, unsupported capability); the client does not skip
it. A root candidate that needs such a capability, positioned before the
first valid root, leaves the choice of root unresolved too, since the client
cannot tell whether it is the first valid one.

A root row the client cannot decode (junk, or a newer format's) makes the
space incomplete for this client: it creates nothing for it, so such a root
blocks the creation of that space on this client for good, never a competing
root. Rows of the member accounts' histories that cannot be decoded do not
make the space incomplete; they only make that account's history invalid.

### 3.4 Members and deliveries

Keys move by delivery only, and nobody asks for them: every member's
runtime derives the work from tables it can read (R12, R14), automatically;
applications and users never handle deliveries.

- **Who lacks a key.** A member **lacks** version `v` when no accepted
  envelope of `v` (a delivery to it, or the `accountEnvelope` of `v`'s
  root or successor that it wrote) names an account epoch of its current
  generation. A delivery at or before the root's position does not count
  here, as in acquiring (§3.3); for key holders and the needs-rotation mark
  (§3.5) it does, which is the safe direction. Versions concerned are the current version and, after a
  recovery rotation (§3.5), the skipped versions. A device-removal account
  rotation changes nothing here: the account's remaining devices open
  envelopes sealed to earlier epochs of the generation through its history
  (§2.12), and the space is marked as needing rotation instead (§3.5). A
  reset starts a new generation, so a reset member lacks every version
  until someone delivers again (§2.11). A member's current generation comes
  from its public roots, which member devices watch (§3.8).
- **Delivering.** Every device holding keys of a space watches its member
  rows, and the deliveries of the versions concerned (the current
  version, and skipped versions it holds), not those of earlier versions
  (§3.8). For each member that lacks a version it holds, when its **pass**
  runs (below), the device checks, from the chain it has
  authenticated for that address and its space root (§3.3), that `v` is a
  version of **that** space's chain, and takes the key from that space's
  held-key document (§6.2) only. It then reads the member's public account
  history (R10), finds its current account epoch and that epoch's valid
  sealing key (§2.12) (without one it waits, watching the member's
  `__e2ee_account_keys` rows until one appears, §3.8), and inserts the delivery
  `{scopeId, identifier, keyEpochId, senderAccountId, recipientAccountId,
recipientAccountEpochId, envelope}`, with a random ID, sealed to that
  key, in a ledger transaction whose read set includes the member's public
  account rows, the member rows and the space's deliveries. Read-set
  validation (§0 item 5) refuses the delivery if the member's account
  rotated or its member rows changed after that cut, so a delivery is never
  sealed to an epoch that a since-removed device still holds, and, since
  deliveries are readable to every member (R14), if another holder's
  delivery landed after the cut (a phantom of the read): the device then
  reads again. **The first holder to commit wins**, with no delay: the loser
  re-reads, finds the member holding the version and stops, so two holders
  racing write one delivery. The authority accepts a delivery only from a
  member to a member (R17). A server that hides a delivery from a holder
  costs a duplicate delivery at most.
- **The pass, and when it runs.** A holder's **pass** over a space finds the
  members that lack a version it holds and delivers it. It covers the spaces
  this device holds the current key of, and every pass reads the space and
  its members' accounts from the authority, so a member the device's store
  had not seen is seen; it compares the members with the deliveries and the
  members' account generations (new members, and reset accounts whose
  delivery is now stale). It runs at natural moments, never on a timer: when
  the app starts or reconnects, once its subscriptions have caught up; right
  after this device changes a space's members itself (so whoever adds a
  member normally delivers at once); when a watched space changes (§3.8),
  for the spaces in use; and whenever the app calls the public method that
  runs it, for every space or for one. Applications should call it after
  adding members. A new member of a space no holder is looking at gets its
  key at the next natural moment of any holder, or when the app runs the
  pass. (A pass that skipped a space nothing had changed in, and a catch-up
  that widened the watch first, were measured at a saving of under half of a
  pass and are not part of the design.)
- **Receiving.** A member's devices watch deliveries addressed to their
  account and acquire the key (§3.3). While its account lacks a version
  that it needs, a member's device reports the space `key-pending` (§4.3).
- **A delivery that does not open.** A delivery whose envelope provably
  fails to open, or whose opened key fails its own version's marker (§3.3),
  is skipped, and the recipient's devices emit (unless it was sealed to a
  sealing key record of theirs that does not derive, which is reported
  against that record instead, §2.12)
  an `invalid-delivery` encryption event `{space, sender, recipient,
keyEpochId}`, once per delivery and device. Nothing is written: holders
  cannot see that the envelope failed, so they count the member as holding
  the version and do not deliver again. The member stays `key-pending` until
  an updater rotates (`db.encryption.rotate(space)`, §3.5), which delivers a
  new version to every member. The application decides how the report
  reaches someone who manages members. A device does not report a delivery
  whose opening failed for any other reason: a provider failure (§0), a
  capability it lacks (§3.3, "Unresolved history"), or an account epoch it
  cannot reach (another generation, which it skips). A key that opens and
  passes its own marker but fails a history link further back is not
  reported against the sender: the fault is in the chain, not in the
  delivery. The device instead emits a `broken-history` encryption event
  `{space, keyEpochId, author}` naming the successor whose `history` failed
  and its author. No key at or after that version can be authenticated
  through it, for any member; the remedy is a recovery rotation (§3.5). Once a device holds a version, it opens no further deliveries
  of it, so later bad deliveries of that version are not reported. A member
  is trusted with the key anyway (it could disclose it); the remedy for a member that
  delivers badly is to remove it and rotate (§8).
- **A member this client cannot evaluate.** When a member's account history
  needs a capability this client lacks, the client skips that member,
  delivers to the others, and reports it as unsupported, never as invalid
  (§7). It does not count such a member towards the needs-rotation mark
  (§3.5): clients that can evaluate the member still do, and counting it
  would make an older client rotate on every write. One such member never
  stops deliveries or rotations for the rest of the space. A member whose
  own account history is an intrinsic contradiction (§7; for example a
  root whose device has no keys) is skipped and reported the same way, and
  nothing it signed is accepted; unlike an unsupported member it counts
  towards the needs-rotation mark (§3.5), since no client can tell which of
  its devices can open what it holds. A member whose account rows are
  missing from the read is a caller error (inconsistent evidence), not
  unsupported.
- **Not a member.** A device whose account has no member row for the space
  (its own member rows are readable to it, R12) reports what it cannot
  decrypt `key-unavailable`: nobody will deliver to it.

Required behavioural tests: a new member receives the current version from a
holder; a delivery naming an epoch of another space is never written; a
delivery to an account without a member row is refused (R17); a reset member
receives the current version again; a delivery whose recipient's account
rotates between the holder's read and its commit is refused and redone
against the new epoch; a member that receives a garbage delivery reports
`invalid-delivery` naming the sender, writes nothing, and receives a working
key after the next rotation; a delivery that fails with a provider error is
not reported; after a rotation, a member that already has a garbage
delivery of the new version also receives the rotator's delivery; in an
otherwise idle space, a member that resets after it was delivered the
current version receives it again with no other write to a space table; a
member whose account sealing key appears only after a holder's first lookup
found none receives the version with no other write to a space table.

### 3.5 Rotation and removal

**Rotation** is by an updater (R16) whose device is active and holds the
current version (it acquires it first, §3.3; if it cannot, `rotate` fails
`rotation-needs-key`, and so does a recovery rotation by a device that holds
no version at all). It generates a new epoch and
`K'`, and inserts, in one ledger transaction that read the chain, the
space's member rows, the chosen members' public account rows and its own
account's public rows at its cut, one successor whose `predecessor` and `historyEpochId` are both the
current version, and one delivery of `K'` to each of at most 100 current
members other than itself that have a valid sealing key (§2.12, §3.4). The
100 are chosen first among members that held the previous version (they
are reading the space now), then among the others, each group in random
order. The successor and the first batch (at least one delivery, unless the
rotator is the only member) commit in one transaction. Right after the
rotation is accepted, the rotator's runtime delivers
`K'` to every remaining member in delivery transactions of up to 100
deliveries, whether or not a delivery to it already exists (another
member's delivery may not open); members it does not reach (without a sealing key, or the
rotator went offline) receive `K'` later from any holder: an unfinished
rotation needs no tracking, since any holder of the new version (the
first-batch recipients, or the rotator on its next start) completes it
through the pass (§3.4), the first holder to commit winning. Read-set
validation refuses a second successor of the same predecessor, and a
rotation whose members changed meanwhile; the loser re-reads.

Rotation does not re-encrypt existing content: old cells stay under their
versions, which the newest key opens. Every rotation adds a version; reads
use the held key of each cell's version and equality queries compute one
token per held version (§4.5). Until a member obtains the new version, it
writes with the newest one it holds (§5.2).

**Needs rotation.** A space **needs rotation** when its current version is
held (§1.1) by an account that is no longer a member, or was sealed to an
account epoch that a later `remove-device` account successor (§2.8)
superseded, so that a removed device can open it. "Later" means anywhere
after that epoch in the account's chain, even with other successors in
between. For an epoch of an earlier generation (the account was reset since),
the removal is looked for in that generation's chain as it stood when the
reset happened; and such an envelope marks the space even without a
removal, since the reset generation's devices (which a reset presumes lost)
can still open it. A rotation answers it by delivering to the account's new
generation. An account whose own history contradicts itself (§3.4, "A
member this client cannot evaluate") also marks it. The mark is derived from tables every member can read (members,
roots, successors, deliveries and the public account successors, R10, R12,
R14); nobody writes it and nobody can forget it.

The mark is conservative evidence from the accepted records' envelope
metadata, not authenticated proof that anyone holds a key: the server checks
no envelope (§1.4) and R17 checks only membership. A current member can
therefore insert a delivery with a junk envelope that names the current
version and an account epoch of another member that a `remove-device`
successor already superseded; the space is then marked although no key was
transferred. This reveals no key to anyone who is not a member; it costs a
rotation, and the member can repeat it for each new version. It is a
member-induced availability and cost limit, like bad deliveries (§3.4, §8).
A member whose own account history contradicts itself (§3.4, "A member
this client cannot evaluate") can do the same without naming anyone else:
a junk delivery to itself names it for the current version, and it marks
the space; again at most one rotation per version.
A rotation clears the mark it answers, since the new version is delivered
to current members' current account epochs only, unless such a record is
written again for the new version.

A member's runtime evaluates the mark when the space's members, the current
version's deliveries or the chain change, and when the public account
history of an account holding the current version changes (a new account
successor, in particular a `remove-device`), even when no space table
changed, at a cost proportional to the members and the current version's
envelopes. Only a device that may act on the mark evaluates it: one that
holds the current version and whose rotation of the space has not been
refused while the client runs (below). Other
devices do not watch the members' account histories for the mark. These are
the subscriptions of [core hooks](E2EE_CORE_HOOKS.md) H6.

Reactions to the mark are bounded: it never triggers a rotation by itself.
A marked space is rotated only on the occasions below, at most once per
mark of the current version, so a forged record causes at most one rotation
per version it names and never an automatic loop.

Required tests: a `remove-device` account successor of a member that holds
the current version, with no change to any space table, marks the space;
so does one separated from the envelope's account epoch by other
successors; a raw delivery with an invalid envelope naming a superseded
account epoch of another member marks the space, and the next encrypted
write by an updater makes exactly one rotation.

**Rotation is lazy and coalesced; never per write.** A space that needs
rotation is rotated:

- before the next encrypted write to it by an updater: the runtime makes
  the rotation in its own transaction and, online, the write waits for its
  outcome within its own deadline and then uses `K'`. If the rotation is
  refused (the writer is not an updater), offline, or not decided within
  the deadline, the write uses the current version. A refusal stops
  automatic rotation of the space on this device (below);
- by `db.encryption.rotate(space)`, at once, when the application wants
  the removal effective immediately.

Several removals before the next rotation are covered by one rotation.
Writes by members who are not updaters stay under the current version until
an updater rotates.

**After a refusal.** When the authority refuses a rotation by this device,
the write path does not rotate the space on this device while the client
runs, and the device stops
evaluating the mark for it. Explicit `db.encryption.rotate(space)` and
`recoverRotation(space)` always attempt. As for creation (§3.2), this is
kept only in memory, so a restart allows one more automatic attempt.

Required tests: a member refused a rotation makes no new automatic attempt
after a further removal; an explicit `rotate` after a refusal attempts.

**Recovery rotation.** When no online member holds the current version (for
example the only holder lost its device right after rotating), or a
successor's `history` is broken (`broken-history`, §3.4), an updater
may call `db.encryption.recoverRotation(space)`. It is never part of
routine rotation, and the SDK never calls it on its own: the application
calls it explicitly, typically after asking a person. It writes a successor
whose `predecessor` is the current version (so it extends the chain) and
whose `historyEpochId` is the newest version the rotator holds (for a
broken history, a version before the broken successor), whose key its
`history` wraps, with deliveries to members as for any rotation. The
versions after that one, up to and including the predecessor, are
**skipped**: the chain gives no key for them (§3.3). A device that already
holds a skipped version keeps it, and delivers it to members that lack it
(§3.4), so content under it remains readable to holders and to the members
they reach. Content written under skipped versions that no member holds is
lost: members report its cells `key-pending` indefinitely. A recovery
rotation needs at least one held version; with none, the remedy is a new
space (§8).

**Removal.** To remove a member, an updater deletes the member's rows
(R12). Jazz then stops sending that account the space's rows (the space
row's read rule comes from members, R13), no delivery to it is accepted
(R17), and the space needs rotation, which happens lazily as above. The
remover is an updater and is online at that moment, so developer
documentation recommends that a removal UI call `db.encryption.rotate(space)`
right after the delete; otherwise content written before the next updater's
encrypted write stays under a key the removed account holds (§8). The
removed account keeps every key it already held, which stay usable for
content of their versions (§8). Removing a device of an account is handled
through the same mark: revoke its sign-in, then the space is rotated (§2.8).

### 3.6 New devices, recovery and account changes

Space keys are held per device and sealed per account. A device approved
(§2.5) or restored by recovery (§2.9) obtains its account's key and then
acquires every space key sealed to its account, from the envelopes of roots
and successors its account wrote and from deliveries to its account (§3.3);
no other account and no other device is involved. An account rotation changes
the sealing key later envelopes use; the account's remaining devices still
open earlier ones. A removed device opens only what was sealed to the
account epochs it held, and keeps the space versions it held until spaces
rotate (§2.8, §2.12). Members deliver again to a reset account (§2.11, §3.4).

### 3.7 Offline

Provisional operations replay over a provisional snapshot (§0), keeping the
author's account-history predicate (active device, not removed, keys and
mechanisms matching). Space creation may be offline, staged in the same
transaction as the encrypted write that needs it, exactly as online (§3.2,
§5.4). Member rows written offline are ordinary writes, submitted on
reconnection. Deliveries, rotation and repair are online only. On
reconnection, Jazz permissions decide acceptance; a refused or lost creation
is repaired as in §3.2 once the device is online and holds the real key.

### 3.8 Supported size and costs

**Limit.** The first release supports up to 1,000 member accounts per space
(distinct `accountId` values of its member rows). It is a documented design
limit, not enforced: neither the client nor the server refuses a member
beyond it (Jazz policies cannot count rows). Every member's runtime that
sees more than 1,000 member accounts in a space emits a
`space-over-member-limit` encryption event `{space, members}` once per space
and keeps working; the costs below then grow beyond what the first release
is designed for.

**What a device watches.** Only spaces in active use are watched: when the
app has an open subscription on an encrypted table, the spaces of its rows
join a shared set, counted per subscription, and leave it when the last
subscription using them closes. The runtime cannot see a subscription open
or close, so the binding that opens it tells the runtime. Per space in the
set: the
space's member rows (at most 1,000), its roots and successors (one per
version), the deliveries of the versions concerned (the current
version, and skipped versions it holds, §3.4), and the public account roots
of its member accounts (one per generation), since who lacks a version
depends on each member's current generation (§2.11). Deliveries of earlier
versions are not watched: the newest key opens them. Its own account's
member rows and the deliveries addressed to its account (of every version)
are watched across spaces. A member's `__e2ee_account_keys` rows are watched
only while a delivery to it waits for a valid sealing key (§3.4), and the
members' public account successors only by a device that may act on the
needs-rotation mark (§3.5); the delivery transaction reads the rest of a
member's account history when it needs it (§3.4). The space watches are shared across
the set too: one subscription per system table (member rows, roots,
successors, deliveries) filtered to the set (`scopeId IN … AND identifier IN
…`, and for deliveries `keyEpochId IN` the current versions: the store serves
that from its `(scopeId, identifier, …)` indexes, and would scan the table for
`identifier IN …` alone), replaced
with the new one opened before the old one closes when the set changes; a
change runs the pass (§3.4) for the spaces of the set. The member accounts of
the spaces of the set are watched too, one subscription per table (roots,
public successors, sealing keys) over them: the rows that who lacks a key, the
needs-rotation mark depend on. Watches of public account
rows are shared across spaces: one subscription per table over the
accounts the device needs, replaced (the new one opened before the old one
closes) when that set changes. The design figure is 10,000 watched accounts
per device (for example 10 spaces at the 1,000-member limit); beyond it the
watches still work, at a cost to be measured. A watch is opened before the decision it guards is
first made, and the decision is made from the watch's results, so no change
between a read and the watch is missed. An account leaves the set when it
is no longer a member of any space that needs it, or its waiting delivery
is made or abandoned.

**Costs at the limit.** In a space of 1,000 members:

- A rotation writes one successor and up to 999 deliveries, in one
  transaction of at most 100 and then batches of 100 (§3.5): about 1,000
  sealing operations on the rotator's device. Every device of a member that
  holds keys of the space receives those deliveries, a few hundred bytes
  each, so of the order of a few hundred kilobytes per rotation, and
  re-evaluates who lacks a key once per batch, at a cost proportional to
  the members.
- Adding a member costs one delivery, by one holder; each holder
  device re-evaluates at a cost proportional to the members. Holders
  racing to deliver write one delivery: the first to commit wins (§3.4).
- Deliveries are never deleted (R1): the server stores one per member per
  version (1,000 rotations of a 1,000-member space leave a million rows),
  but a device syncs only those of the current version and of skipped
  versions it holds, plus every delivery addressed to its own account.
- A device removal marks every space whose current version the removed
  device could open; each is rotated by the next encrypted write of an
  updater (§2.8), at the cost above.
- A device watches the public account rows of every member of every space
  it belongs to (above); the design figure is 10,000 watched accounts per
  device.
- On the server, reading an internal row and inserting a delivery each cost
  one or two indexed member lookups; inserting a root, successor or member
  row also evaluates the space row's read rule and both update clauses
  ([core hooks](E2EE_CORE_HOOKS.md) H7, Main risks).

These are design figures, to be measured before landing ([core
hooks](E2EE_CORE_HOOKS.md), Main risks). A later release may raise the
limit once measured.

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
shorter than the smallest cell record: 78 bytes in all, the 31-byte common
envelope header of `jazz.e2ee.cell-record` v1 and a 47-byte payload. Two such values have fixed
meanings and are never reported as malformed:

- The **empty byte string** means "never written": the row predates the
  column, which a lens added with the empty default. The reader returns the
  column's declared plaintext default (`{ "type": …, "default": … }` in the
  encryption entry; it is stored and deployed in plain text, visible to the
  server, so it must not be secret), or null for an optional column. When
  the empty value is stored in the row (rather than supplied by the lens for
  a row that predates the column), any writer Jazz permits could have put it
  there, so the row is returned with the default and the cell is reported
  `empty-cell`; the read does not fail. Core tells the two apart (H3).
  A required column that declares no default has no value a never-written
  cell could read as (deploy refuses to add one to rows already stored), so a
  row whose cell of such a column is the empty byte string is left out of the
  read and the cell is reported `empty-cell`; the other rows are returned.
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
  otherwise a live query pays the acquisition again on each refresh. A cell
  whose epoch could lie beyond the point where the chain is unresolved (§3.3)
  has no data-error code of its own: it is reported with the unresolved
  chain, as an unsupported capability (§7), never as `invalid-ciphertext`.
- A member of a space that has no valid root yet reports it `space-pending`.
- When the acquisition obtains no key for a needed version and the account
  is a member (§3.4), the read returns the rows its held keys decrypt,
  leaves the space's other rows out and reports the space once as
  `key-pending` (§4.3). It does not fail and reports no row
  `key-unavailable`. When the delivery arrives, the device acquires the key,
  the report is withdrawn and live queries receive the rows as additions.
- When the account is not a member, the read returns what its held keys
  decrypt and reports the rest `key-unavailable`. Any other acquisition
  failure (provider error, cancellation) fails the read.
- A held-key record that fails its checks (§6.2) is skipped and reported; the
  read continues with the other held keys and acquires that version again if
  it can.
- Keys obtained by an acquisition, including earlier versions from the
  history chain, become held keys.
- A device watches deliveries addressed to its account and the chain of each
  held space; when a delivery or a new version syncs in, it acquires
  without waiting for a read. A failed acquisition is
  retried with backoff until the key is held, a later version supersedes it,
  or the session closes.

### 4.3 Unreadable rows

An ordinary query or subscription leaves out a row with an undecryptable
encrypted cell and returns the rest; no partial plaintext is returned. Report
`{table, id, column, code, space}`:

| Code                 | Meaning                                                                                                                                                  |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `invalid-ciphertext` | Authentication failed; the cell belongs to another row, table, column or space; malformed; no such epoch in the space's chain                            |
| `unsupported-format` | Unknown envelope mechanism or version                                                                                                                    |
| `key-unavailable`    | This device lacks that version's key and its account is not a member of the space (§3.4)                                                                 |
| `key-pending`        | Per space, not per row: this account is a member and lacks a version it needs; a member's delivery is awaited (§3.4)                                     |
| `space-pending`      | Per space, like `key-pending`: the space row has no valid root yet, and this device's creation was refused while the client runs, or not yet made (§3.2) |
| `empty-cell`         | The stored cell is the empty byte string; the row is returned with the column's default (§4.1)                                                           |
| `superseded`         | The cell holds the cleared marker (§4.1) and this reader's declaration names no conversion for the column                                                |
| `stale-space`        | The cell was sealed under another space of the same row's history; it was read and needs repair ([migrations](E2EE_MIGRATIONS.md) §3.9)                  |

A `key-pending` or `space-pending` report carries `{table, space, code}` with
no `id` or `column`, and is deduplicated by `(table, space)`; it is not repeated for
each row. It is withdrawn when the key is obtained, and live queries then
receive the space's rows as additions. A `stale-space` row is returned (it
decrypted) and reported. A row whose space coordinate is not a string is
reported against the space column as `invalid-ciphertext`. Any other error fails the read. Reports are
deduplicated per account by `(table, id, column, code)` in a bounded
least-recently-used set of 10,000 entries; a listener registered later first
receives the reports still in that set. A console warning names table, row and
column, never content. The reports belong to the client runtime and its listeners: a read's result, one-shot or a subscription's, carries none, and the database core does not pass them on.

Unreadable rows are dropped after `limit` and `offset` have been applied, so a
page may contain fewer rows than its limit. This is accepted behaviour: the
dropped rows are reported, and a client must not treat a short page as the end
of the results.

### 4.4 Strict reads

Reads inside a transaction, mutation pre-image reads and authority reads are
strict: any outcome that would leave a row out fails the read, so a write
never rests on skipped rows. Rows returned with a report (`empty-cell`,
`stale-space`) are returned as in any read.

### 4.5 Equality-filtered queries

Encrypted predicates must be `eq` (or a complete enum `match`) on a column
declared with an equality index, together with a plaintext `eq` on the space
column. Tags are per key version: a query computes one token per held
version, under the selected equality mechanism only, and the physical
predicate becomes `__e2ee_eq_<column> IN tokens`
(the server can link those tokens to one another, §8). Limit and offset are
applied after each candidate is decrypted and its authenticated value
compared. If the version set changes during the query it is retried (shared
budget of 3 attempts). Offline, no tokens are computed: every candidate of
the space is decrypted and compared. Ordering by encrypted columns,
unions and partial selects are refused as unsupported; they never fall back
to a scan. Joins, reachability and inheritance compare plain columns only:
core refuses every condition of them that names an encrypted column, and the
rest (the notes of the projects I am a member of, the projects that have a
note) pass and return decrypted rows. Includes of encrypted child tables are refused
too (a project list with its latest task takes two queries); developer
documentation should say so.

**Cold queries.** Online, a query with an encrypted predicate on a space for
which this device holds no key cannot compute any token, and an empty
`IN ()` would return a false empty result without ever revealing a missing
version. Planning therefore reports the missing space key, and the runtime
acquires it (§3.3) within the read's own budget before executing;
acquisition outcomes are those of §4.2 (a member that lacks the key gets
`key-pending` and no rows). The query then runs against the version set it was planned
with.

**Conversions.** On a column being converted ([migrations](E2EE_MIGRATIONS.md)
§3.3), a query matches a row by its logical value: the source when present,
otherwise the target. It covers source rows by index only when the
conversion is the identity or declares a bounded, exact reverse mapping;
otherwise it fails with `conversion-pending` while rows of the space still
have a present source.

**Completeness.** An equality query returns matching rows of the versions
this device holds. When the device knows (from the chain it last read) that
the space has versions it does not hold, the result may miss rows of those
versions, and the query reports the space once as `key-pending` (a member)
or `key-unavailable` (not a member) so the result is not mistaken for
complete.

### 4.6 Subscriptions

A subscription whose space is `key-pending` waits without emitting, in one
bounded episode (the operation timeout, 10 s by default). Delivery or
authority changes trigger fresh reads; there is no polling and retries do not
extend the deadline. On expiry it emits the rows its held keys decrypt,
reports the space `key-pending`, and keeps watching; the rows arrive as
additions once the key is obtained. The members who can deliver may be
offline for much longer than one episode, so the wait is reported, not
treated as an error. One-shot reads do not wait.

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
write that needs a key its member account lacks waits for a delivery within
its own deadline. A write to a space that needs rotation may first rotate
it (§3.5).

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
key with the greatest chain depth, never one marked `refused` (§5.4). A held key's `position` always means its
version's depth in the chain (root = 0); one chain orders by depth alone, so
the ledger position is not needed. When acquiring, the device starts from
the current version and, if it cannot open that version's key (its delivery
has not arrived), steps back through predecessors; a version not yet
delivered therefore does not block writes. Equality tokens use the same key
and version. Once the device has read the space's chain, only keys of that
chain's root write. Before such a read in this session (the chain read is
kept only in memory, so also after a restart), a device that holds keys of
more than one root of the address not marked `refused` cannot tell which
root is the space's: the runtime reads the space's chain and retries the
write at once, and offline reports it `space-pending`. Keys of one root
alone write without a read.

### 5.3 Offline and provisional writes

A write is provisional exactly when reads would use retained local state.
With a held key, an offline write records no access predicate. Offline space
creation keeps the author's device-active predicate (§3.7).

### 5.4 Space creation, including offline

An encrypted write to a space with no known valid root stages the root in
the same transaction (§3.2), online or offline; the authority accepts it
only from an updater (R15), and a refusal fails that transaction alone. The
new key is stored as a held key before commit, with a persisted
`unpublished` mark (§6.2) carrying the creation's durable transaction
identity, and the mark is cleared once the creation is accepted as the
space's root. An unpublished key is never chosen over a key of another root:
a device holding both writes only after it has read the space's chain, and
then only under the chain's root (§5.2), including after a restart.

Later writes under an unpublished key are ordinary transactions of their
own. They do not depend on the creation and are not held back or refused
with it: Jazz permissions alone accept them (§5.1). A device does not stage
a second root for a space while it holds an unpublished key for it.

The creation's outcome decides the key's fate:

- **Accepted, and the space's root** (§3.2): the mark is cleared. Until a
  valid root of the address is readable, the device cannot tell, so the
  mark stays `unpublished`.
- **Not accepted** (the root was refused, or its transaction failed
  otherwise, at Local or Global, or was cancelled before possible
  submission, §3.2), or **accepted but lost** to an earlier valid root: the
  record is rewritten with the mark `refused`. Only a refusal of the root
  itself also stops automatic creation of the space (§3.2). The key stays usable
  by this device for reading its own rows and for the repair of §3.2
  (obtaining the real key and rewriting the cells it wrote under this one),
  and is never used for a new write. The record is kept (§3.2).
- **Unknown**: the mark is kept.

The transaction identity the runtime stores is the ID of the root row it
staged: the row's ID is chosen before the transaction exists, and the key is
stored before commit, while the transaction's own number is drawn at commit.
The runtime finds the transaction through that row in its own store: the row
pending (`Own(tx)`, then `transaction_fate(tx)`), accepted, or absent. A root
absent from the store is looked up among the commits the store keeps as
refused (a root refused after the run staged it is found without a
restart); if it is not there and this run is still staging it, it is
undecided; otherwise only an online read of the authority decides: the root
shown there is accepted, its absence is `not accepted`, and offline, or with
rows of the address this client cannot decode, it stays undecided. The local
store alone never makes a creation `not accepted`, since a store that lost
the commit says nothing of the authority's decision. The authority's refusal
names no row: a refusal (`authorization-denied`) of a transaction that
carries this device's root is taken as the refusal of the root (§3.2, one
automatic attempt per space after a restart, then the automatic paths stop
again), whatever else the transaction held; a transaction that fails for
another reason stops nothing.

After a restart the runtime reconciles each `unpublished` record by its
stored transaction identity: accepted clears the mark (or marks it
`refused` when another root won), refused marks it `refused`, pending and
unknown keep it; if the identity is unknown to the store and a valid root of
the address is readable, the mark is cleared when it is this record's root
and set to `refused` otherwise. Reconciliation never stages another root.

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
- Space creation commits with the first encrypted rows that need it, or
  alone for `createSpace` (§3.2).
- **Keys inside a callback transaction.** A staging call that needs a key
  this device does not hold returns "needs key". Core's `Db::transaction`
  takes an asynchronous callback, so the staging call awaits acquisition
  (§3.3) inside it and retries; the callback itself is not replayed. A
  binding that exposes a synchronous callback (one that cannot await)
  instead fails such a staging call with `key-pending` (or `space-pending`
  when what is missing is the space's root) and aborts the transaction; the application acquires keys first with
  `db.encryption.acquire(space)` (which also prepares a missing space's
  root unless its creation was refused on this device while the client runs, §3.2, and makes a pending rotation, §3.5) and runs the transaction
  again.
- Unknown outcomes are reconciled by the original transaction's identity, not
  by resubmitting.

## 6. Private local state

### 6.1 Store

Private state is kept in a store separate from authentication storage. Per
scope `Dₐ(account)` it holds one device document and one held-key document
per **space root** the device holds keys for. Held keys are kept per root, not per address, so the key of a
lost or refused root under repair (§3.2) and the keys of the address's real
root are durable side by side. Storing a key rewrites only that root's
document:

```text
device document:
{ "format": "jazz-e2ee-local-devices-v3",
  "devices": [ { scope, id, mechanism, version, publicKey, privateKey,
                 signingMechanism, signingVersion,
                 signingPublicKey, signingPrivateKey, approval } … ] }

approval (the device's approval nonce, §2.2):
{ "format": "jazz-e2ee-approval-nonce-v1", commitment, nonce }
  until the device answers a challenge, then
{ "format": "jazz-e2ee-approval-nonce-v1", commitment, "spent": true }

held-key document (one per space root):
{ "format": "jazz-e2ee-held-space-v1", "revision": n,
  "lineage": <lineage record>, "keys": [ held-key record … ] }

```

`devices` holds at most one device, the one of the document's scope. An
unknown format is refused and never overwritten. The document is canonical
JSON (§6.2), bytes as arrays. `commitment` is the request's
`nonceCommitment`; `nonce`, like the private keys, is stored only as the
platform's protected blob (below). Before a device reveals its nonce in a
proof (§2.5, §2.9), the runtime rewrites the document with `spent` and no
nonce, and waits for the store to confirm it; a device whose document says
`spent` refuses every challenge. A copy of the store made before that is
bound to the platform's protection like the private keys: where the
protection does not travel with a copy (below), neither does the nonce.

Device private keys are never stored in the clear:

- **Browser:** each private key is wrapped with a non-extractable WebCrypto
  key kept in IndexedDB; scripts can use the wrapping key through WebCrypto but
  cannot export it.
- **Apple platforms:** Keychain. **Android:** Android Keystore.
- **Other platforms:** app-supplied secure storage.

### 6.2 Held-key records

Each held-key document stores the space's root and lineage once, and one
small record per held version that refers to it by depth:

```text
lineage record:
{ format: "jazz-e2ee-held-lineage-v1",
  binding: { application: Dₐ(self), device: deviceId, keys: <device hash> },
  root: <space root row, bytes as arrays>,
  successors: [ <space successor row>, … ],
  signature: [..] }

held-key record (one per epoch):
{ format: "jazz-e2ee-held-key-v3",
  binding: { application: Dₐ(self), device: deviceId, keys: <device hash> },
  rootId, epochId, depth: "<decimal>",
  prefix: <lineage prefix hash>,
  unpublished?: { transaction: "<durable transaction identity>" },
  refused?: true,
  envelope: [..], signature: [..] }
```

`canonical` sorts object keys by code unit, drops undefined, and
encodes bytes as arrays and big integers as decimal strings; `B(f, x) =
UTF8(json(canonical([f, x])))` with `f` the record's format. `sha256hex` is
the lowercase hex of SHA-256. `<device hash>` is
`sha256hex(B("jazz-e2ee-held-device-v1", [publicKey, signingPublicKey,
envelopeMechanism, signingMechanism]))`, the two public keys as their raw
bytes (as in the device document) and each mechanism written as the object
`{ "id": <mechanism id>, "version": <version> }`. `root` is the space root
row and each of `successors` a space successor row, each a JSON object whose
fields are the row's columns as named in §1.3 plus `id` for its row ID,
`version` a number; the creator's domain is derived from `creatorAccountId`
as elsewhere. `successors` are the accepted successor rows from the root's
epoch onward in chain order, as far as the deepest held epoch, appended as
deeper keys are kept. A held-key record names its exact `epochId`, its
`depth` (§5.2) and the hash of the lineage prefix up to it,
`<lineage prefix hash>` = `sha256hex(B("jazz-e2ee-held-prefix-v1", [root,
[s₁, …, s_depth]]))`, where the list holds exactly the first `depth`
successors in chain order (at depth 0, the root and an empty list). `signature`, `envelope` and every bytes
column of a stored row are complete common envelopes (§0), stored as byte
arrays. The lineage record is signed by the device over `B(format, record
without signature)`.
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
keys and the space is acquired again. When the device rewrites the document,
a held-key record skipped as unsupported is carried over unchanged, since it
may hold a key only this device has, such as a `refused` key written by a
newer client; a record that failed any other check may be dropped.
Unsupported here means a record of this format whose envelope or signature
uses a mechanism this client lacks. A newer client that changes the
record's encoding uses a new document format, which older clients refuse
and never overwrite (§6.1); a record of this format that does not decode
is malformed and may be dropped. Nothing in either record depends on the
application schema, so a schema update never invalidates held keys; the cell
context binds table, column and type (§4.1). Keeping a key replaces any
record of the same device, root and epoch. `unpublished` is present only for
a space creation not yet accepted (§5.4), carries the creation's durable
transaction identity for reconciliation after a restart, and is removed by
rewriting the record. `refused` marks the key of a refused or lost creation
(§3.2, §5.4): it is kept for good, is never chosen for a write (§5.2), and
still opens the cells this device wrote under it. The lineage records also
give the runtime its epoch-to-space lookup ([migrations](E2EE_MIGRATIONS.md) §3.9).

### 6.3 Binding

All private records are bound to the application id, the environment and the
account (`Dₐ(account)`), not to the server URL. One install pointed at two
datasets with the same application id and environment shares device keys and
held keys.

### 6.3a Floors

The runtime persists, next to the held-key documents of each scope `Dₐ(self)`,
a **floor** for every account whose state it has verified: the cut,
generation, canonical root of that generation and its device's keys, and
current epoch with its position (§2.4, "Floors"), for the device's own account and for every
account it replays (authors, delivery recipients). Spaces have no floor: a
space's state comes from the server, which E2EE does not protect against
(§8).

Each floor is signed by the device like a lineage record (`B(format, …)`,
format `jazz-e2ee-floor-v1`) and advances only forward, after a verification
at a newer cut:

```text
{ format: "jazz-e2ee-floor-v1",
  binding: { application: Dₐ(self), device: deviceId, keys: <device hash> },
  account, through: "<decimal>", generation: "<decimal>", rootId,
  rootKeys: { publicKey, mechanism, version,
              signingPublicKey, signingMechanism, signingVersion },
  epochId, epochPosition: "<decimal>", activeDevice?, signature: [..] }
```

A floor record is accepted on load only if its binding is the current
device's, `account` is the account it is stored for, and its signature
verifies; one that fails is skipped and reported, like a held-key record. Every later read is checked against the floor before its
state is used: older evidence (an earlier cut, a lower generation, a chain
that no longer reaches the floor's epoch) is refused with `stale-state`, and
the runtime reads again; it never decrypts, writes or delivers with the older
state. An account whose chain no longer contains the floor's root is
`root-not-mine`, and an own-account floor whose device is no longer active is
`device-no-longer-active` (§2.4). Floors are wiped with the held keys (§6.4).
Protection starts at the device's first observation of each account:
before it, the device has nothing to compare with (§8).

### 6.4 Wipe rules

- Normal sign-out retains device and held-key records, so a later offline
  sign-in can read.
- With `clearOnSignOut`, the **last tab out** removes the account's device
  record and every held key bound to it (for a device-account store, together with
  the store's deletion). A tab that signs out while another holds the store
  keeps them. The wipe deletes the client's core storage first and the
  private E2EE records second; if the second step fails, sign-out reports an
  unfinished wipe.
- `deleteClientStorage` removes the namespace's storage and the registered
  private records, and invalidates live handles. It revokes nothing remotely.
- Access changes never remove held keys. Records are removed only by
  sign-out wipe, account change wipe, device removal, re-enrolment after
  `device-no-longer-active` (§2.4, "Floors": the account's device record,
  held keys and floors) and explicit deletion. A refused or lost creation's
  key is kept (§3.2). A wiped store is never recreated by a late operation.
- A held-key revision counter advances on every removal, so in-flight reads
  notice and retry (3 attempts).

## 7. Failure taxonomy

Every operation keeps its original error. Publication errors carry the
original record IDs and, when the refusal has a specific cause, that cause's
code (for example `recovery-root-mismatch`); a specific failure is never
flattened into a bare `refused`. The data-error codes are exactly
`key-unavailable`, `key-pending`, `space-pending`, `unsupported-format`,
`invalid-ciphertext`, `empty-cell`, `superseded`, `stale-space` and
`encryption-failed`. The encryption status events (not data errors) are
`account-reset` (§2.11), `invalid-delivery` (§3.4), `broken-history` (§3.4),
`conflicting-account-key` and `unsupported-account-key` (§2.12) and `space-over-member-limit` (§3.8). Account roots that do not count are reported with
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

| Outcome                     | Codes                                                                                                                                                                                            | Must                                                                                                | Must not                                                                                       |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| Device approval pending     | None                                                                                                                                                                                             | Show "waiting for approval or keys"; complete approval or recovery                                  | Re-approve, or treat the account as signed out                                                 |
| Key pending                 | `key-pending`                                                                                                                                                                                    | Report the space `key-pending`; subscriptions wait one bounded episode, then emit and keep watching | Poll, fail the read, or report each row `key-unavailable`                                      |
| Key unavailable             | `key-unavailable`                                                                                                                                                                                | Return what held keys decrypt; report the rest                                                      | Write plaintext, or wait for a delivery                                                        |
| Space pending               | `space-pending`                                                                                                                                                                                  | Refuse the write `space-pending`; report the read; watch for the root (§3.2)                        | Retry a refused creation automatically in the same run, or write plaintext                     |
| Superseded / stale space    | `superseded`, `stale-space`                                                                                                                                                                      | Report; leave a `superseded` row out; return a `stale-space` row                                    | Report either as `invalid-ciphertext`                                                          |
| Permission refusal          | Publication outcome `refused`, with its specific code                                                                                                                                            | Report; reflect in local state                                                                      | Treat key possession as overriding it                                                          |
| Authentication failure      | `invalid-ciphertext`                                                                                                                                                                             | Leave the row out (ordinary reads) or fail (strict reads)                                           | Return unverified plaintext                                                                    |
| Unsupported format          | `unsupported-format`                                                                                                                                                                             | Report                                                                                              | Treat as invalid ciphertext                                                                    |
| Encryption failed           | `encryption-failed`                                                                                                                                                                              | Fail the write                                                                                      | Submit plaintext                                                                               |
| Provider failure            | The mechanism's `CryptoFailure` (distinct from `InvalidCiphertext`, `InvalidInput`, `NoKeyForMechanism` and a `false` verification), and a writer's failure to read back output it just produced | Fail the operation loudly; preserve the error                                                       | Report it as key unavailable, corrupted ciphertext or an invalid proof; try the next candidate |
| Unsupported capability      | `ledger-version`, `signing-mechanism`, `signing-version`, `account-key-envelope`                                                                                                                 | Leave the dependent state unresolved                                                                | Treat the record as invalid                                                                    |
| Intrinsic contradiction     | `conflicting-roots`, `conflicting-device-keys`, `missing-root-key`, `missing-signer-key`                                                                                                         | Fail current replay; skip only a candidate decided at a fixed signed cutoff                         | Pick one of the conflicting rows                                                               |
| Unknown publication outcome | Publication outcome `unknown`                                                                                                                                                                    | Reconcile the original operation by its record IDs                                                  | Resubmit blindly or replay a callback or source                                                |
| Recovery failures           | `recovery-cancelled`, `recovery-material-unusable`, `recovery-root-mismatch`, `recovery-delivery-missing`, `recovery-delivery-unusable`                                                          | Only candidate failures permit trying another delivery                                              | Attach private material to the error                                                           |
| Unsupported shape           | An unsupported-query error naming the operation                                                                                                                                                  | Name the operation                                                                                  | Fall back to a scan                                                                            |
| Timeout / cancellation      | Timeout, cancellation                                                                                                                                                                            | Erase owned secrets; stop unsubmitted work                                                          | Claim a submitted operation failed                                                             |

Publication stages: account `request`, `bootstrap`, `challenge`, `proof`,
`approval`, `account-rotation`, `account-key`, `recovery-root`,
`recovery-delivery`, `account-reset`; space `create`, `rotate`,
`delivery`, `write`.

`conflicting-account-key` is a report, not a contradiction: a valid sealing
key record of the device's own account that does not derive from the
account key is ignored by that device and reported (§2.12).

The missing-key contradictions: `missing-root-key` (an accepted account
root names a device with no published device keys, §1.2),
and `missing-signer-key` (a signer that is active at a signed record's
position has none). Replay cannot verify such records and fails closed. An
approval whose device has no keys is not a contradiction: it cannot be
verified, and is skipped (§2.4 step 2).

Serialization and transaction conflicts are retried internally only by
operations that are safe to repeat because they re-read their evidence and
stage fresh rows: the runtime's own publications (space creation outside an
application transaction, rotation, deliveries and the repair of refused or
lost creations, §3.2), conversion sweeps, and a staging call retried after "needs key" before submission
(§5.6). A public mutation or transaction callback is never retried or
replayed: its conflict is returned to the caller, and an unknown outcome is
reconciled by its transaction identity.

Refusal codes:

| Code                               | Refusal                                                                                                                                                                   |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `account-has-history`              | Bootstrap found existing account evidence (§2.3).                                                                                                                         |
| `device-identity-conflict`         | A device request or key row exists with different keys (§2.2).                                                                                                            |
| `device-keys-mismatch`             | The device's keys do not match its accepted request.                                                                                                                      |
| `no-device-request`                | Bootstrap or reset found no request (or more than one) for this device (§2.3, §2.11).                                                                                     |
| `reset-device-active`              | An account reset by a device that is still active in the current generation (§2.11).                                                                                      |
| `device-not-active`                | The acting device is not an active device of the account.                                                                                                                 |
| `device-removed`                   | The device was removed and needs a new device ID.                                                                                                                         |
| `stale-challenge`                  | The challenge is unknown, for another device, or not of the current account epoch (§2.5).                                                                                 |
| `approval-closed`                  | The approval handle was closed or belongs to another operation.                                                                                                           |
| `invalid-possession-proof`         | The requesting device's possession proof failed (§2.5).                                                                                                                   |
| `invalid-challenge`                | A challenge not signed, over the requesting device's own keys, by an active device of the account (§2.5).                                                                 |
| `target-not-active`                | The device or recovery root to remove or retire is not active (§2.8).                                                                                                     |
| `self-removal`                     | A device tried to remove itself; another active device must remove it (§2.8).                                                                                             |
| `no-other-device`                  | A removal request with no other active device to complete it (§2.8).                                                                                                      |
| `unsupported-recipient`            | A recipient device, recovery root or account has no key of the selected mechanism (§0, §2.12).                                                                            |
| `account-reset-not-allowed`        | An account reset by a device-account session, or where the application has not enabled `allowAccountReset` (§2.11).                                                       |
| `space-pending`                    | A write to a space with no valid root after this device's creation was refused in this run (§3.2), or offline with competing roots before a chain read (§5.2).            |
| `space-create-refused`             | The authority refused the root itself, staged with a write or by `createSpace` (§3.2); only that transaction fails, and the device stops automatic creation of the space. |
| `rotation-needs-key`               | A rotation by a device that holds no authenticated key of the current version (§3.5), or a recovery rotation by one that holds no version at all.                         |
| `device-account-identity-mismatch` | A device-account root written with an identity whose key does not derive the account ID (§2.3).                                                                           |

## 8. Security properties and limits

**Trust boundary.** Jazz permissions alone decide who may read a space: its
member rows, from which the space row's read rule must come (R13), the
space row's update rule, which is the rule for who manages members (anyone
who may update the space row may add any account, §1.5), and the space
row's creator, who may add itself in the transaction that creates the
row (R12). E2EE protects content against
ciphertext leaking outside Jazz: a leaked database or backup, a copied
replica, or an operator who reads storage. It does not protect against a
server, an operator or a permission bug that adds a member row (that
account then receives the keys, §3.4), nor against a compromised login
(whoever signs in as an account with an active device, or resets an app
account, is that account).

Properties:

- Only devices that obtained a version's key can read cells of that version.
  Cells and streams are bound to application, environment, space, table,
  row, column, version and logical type; moving ciphertext between any of
  these fails authentication. Equality tokens are bound to the same
  coordinates except the row, by design: equal values of one column and
  version match across rows ([equality format](E2EE_EQUALITY_FORMAT.md),
  Context).
- Signed records (device-account roots, public device approvals,
  account successors, recovery roots, account sealing keys, private
  approvals and device proofs, space roots and successors) are signed by
  their author device and replayed in accepted order; forged or reordered
  records are skipped, contradictions fail closed. The other records are
  not signed: `app` account roots (trusted on first use, below), account
  identities, challenges and every envelope record (device, recovery,
  successor and space deliveries) are authenticated
  by Jazz authorship (`$createdBy`) and by the markers and chains their keys
  must verify against. A delivered key is used only after
  it authenticates against its version's marker and the signed chain (§3.3).
- **Removal excludes from later keys, if the server enforces permissions.**
  Once an account has no member row and the space has rotated, honest
  members' clients deliver it no later version, and the server refuses any
  delivery to it (R17), so it cannot decrypt content written under later
  versions. Member rows are not signed: this holds only while the server
  enforces the derived policies (trust boundary above).
- Unsupported or unverifiable evidence is never treated as invalid, and
  provider errors are never treated as proof of invalidity.
- Error messages carry fixed text and public coordinates, never plaintext,
  keys or provider diagnostics.

Limits:

- **A bad sealing key record works until its signer is removed.** Senders
  cannot check derivation (§2.12), so while a non-derived record of an
  account is the first valid one, they keep sealing to it, and a correct
  later record does not replace it. In that window a delivery that was
  deliberately broken is also attributed to the record, not to its sender.
  Removing the signing device ends the window.
- **A copy of an unapproved device's store can answer one more challenge.**
  A copy of an unapproved device's store taken while its protection travels
  with it (an app-supplied file protector, a VM snapshot) can answer one more
  challenge with an already revealed nonce, which lets a server that
  substitutes keys pass the approval code for that one approval. Where the
  protection is bound to the device, a copy opens nothing and answers none
  (§2.5, §6.1).
- **Older clients may miss a removal.** A client that cannot read a
  member's account history (a newer format) does not rotate when that
  member's device is removed, and does not deliver to it (§3.4). Clients
  that can read it do. If every updater of a space runs such an older
  client, the space is not rotated for that removal until one updates.
- **A client that cannot read a key change keeps sealing under the key it
  holds.** A client that cannot read a newer successor of a space (a newer
  format, or a signing mechanism it lacks) seals new content under the
  newest key it holds; a held key neither permits nor forbids a write
  (§5.1). If that successor rotated the key away from a removed member, the
  removed member may read the content such a client writes until the client
  updates.
- **Removal is detected from member rows; rotation is lazy.** Deleting a
  member's rows marks the space as needing rotation (§3.5); no `rotate`
  call is needed. The new key is made before the next encrypted write by an
  updater, or at once by an explicit `rotate`. Until then, content written
  by members (and by updaters offline) stays under a key the removed account
  holds; Jazz stops sending it rows, but a database leak would expose that
  content to it. Developer documentation recommends that removal UIs call
  `rotate` right after removing a member (§3.5).
- **Managing members is updating the space row.** Every session that may
  update the space row may add any account as a member and remove any
  member (R12). An application cannot let members edit the space row
  without also letting them manage members; such fields belong in another
  table (§1.5).
- **A member row written later re-adds the account.** Removal deletes the
  member rows the remover sees. A member row inserted at the same time, or
  written offline before the removal and synced after it, makes the account
  a member again, and it receives later versions. Removal UIs should check
  the member list after syncing.
- **A device that had the recovery secret may keep it.** Removing a device
  does not retire a recovery root, and a device already active that runs
  restore writes nothing. For an externally authenticated account, to revoke
  the recovery secret, retire its recovery root and register a new secret
  (§2.8, §2.9). For a device account the recovery secret is the account
  secret, which is the account (§2.9, §2.11): removing a device does not lock
  out a holder, because the device re-enrols and restores from the secret at
  its next start (§2.4) and receives later keys; retiring the root does not
  help, because the holder can still act as the account (for example, add an
  account it controls to the spaces the account may change). If the secret
  may have leaked, the only remedy is a new account.
- **A creation of unknown outcome keeps its key.** If neither the
  transaction's outcome nor a root of the address can be learnt, writes
  keep sealing under the unpublished key; other members report them
  `invalid-ciphertext` until the outcome is known and the rows are repaired
  (§3.2, §5.4).
- **Busy large spaces may delay rotation.** A rotation's read set covers the
  space's member rows, so in a space with frequent membership changes it may
  conflict and retry until they pause (§3.5).
- **Removal cannot erase copies.** A delivered key is never revoked. Keys,
  plaintext and ciphertext a former member already obtained remain with it,
  and old versions stay readable to whoever holds their keys.
- **New members get the full history.** The newest key opens every earlier
  version, except versions skipped by a recovery rotation (§3.3, §3.5), which
  only their holders' deliveries give. So anyone added as a member can
  decrypt everything ever written in the space, apart from content under
  skipped versions that no member still holds. Content that a later member
  must not see belongs in another space.
- **Keys need a key holder's client online.** Nobody delivers keys but
  members' clients (§3.4): a new member, a reset member, or a
  member who missed a rotation's delivery waits, `key-pending`, until a device of a
  member holding the key is online. A rotation delivers to at most 100
  current members in its own transaction and to the rest in later batches
  from the rotator; members without a valid sealing key, or not yet reached
  when the rotator went offline, wait for any holder (§3.5). So the wait
  falls mostly on members added later, on large spaces, and on members
  whose account has no sealing key yet.
- **Losing the current key needs a recovery rotation, and loses content.** If
  every holder of the current version loses it before anyone else obtains it
  (for example the rotator loses its only device before any other member
  had a sealing key), nobody can deliver it or rotate routinely. An updater
  can then make a recovery rotation (§3.5), linking the new version to the
  newest version it holds: rotation and delivery resume, but content written
  under the skipped versions is lost to every member who did not hold them,
  and stays `key-pending` for them. With no version held by any updater, the
  only remedy is a new space, which means a new space row: a new row ID,
  every encrypted row copied into it with new IDs, and every reference to
  the old row and its rows rewritten by the application. The same holds
  after a rotation by an updater whose history is wrong (a client defect or
  bad faith; updaters are trusted to rotate).
- **Membership is the content's confidentiality rule.** Finer application
  rules inside a space are not cryptographically separated (§1.5). Further
  conditions in the space row's read rule (session claims, row columns)
  narrow which members receive rows from Jazz, not which receive keys: every
  member holds them, and keys are delivered per account (§2.12), so every
  session of a member account can decrypt ciphertext obtained outside Jazz.
- **Members see each other.** Member rows and deliveries are
  readable by every member (R12, R14): members learn who else is a member,
  who holds which version, who delivered it and when, and from public account roots whether a member reset (§2.11).
- **Space keys are shared by an account's devices, and device removal
  withholds only future keys.** Deliveries are sealed per account (§2.12):
  every active device of the account opens them, and device approval is the
  only per-device gate. A removed device keeps every account key and space
  version it held, and while its sign-in stays valid Jazz keeps sending it
  rows written under those versions. Removal cuts it off only after its
  sign-in is revoked and the spaces are rotated (§2.8); spaces the account
  may not rotate are marked and stay readable to it until an updater writes
  to them or rotates them.
- **Members can deliver badly.** A member can deliver keys that fail. The
  recipient's devices report `invalid-delivery` naming the sender (§3.4),
  but holders still count the recipient as holding the version, so it
  stays `key-pending` until an updater rotates. A member delivering badly on
  purpose can repeat this for each new member and each version until it is
  removed. A member
  can also mark
  the space as needing rotation with a junk delivery naming a superseded
  account epoch (§3.5), which costs one rotation per version it names. A member
  whose own account history contradicts itself can do the same with a junk
  delivery to itself. The
  mark is conservative evidence, not proof of key possession. The
  application's remedy is removing that member.
- **Supported size.** The first release supports up to 1,000 members per
  space. Nothing enforces it (§3.8); beyond it, costs grow past what the
  first release is designed for, and members' apps are told
  (`space-over-member-limit`).
- **A space's chain is the one the server last showed.** Spaces have no
  floor (§6.3a), but a read never rolls the chain a device knows back: one
  that shows a prefix of it, or the same chain again, changes nothing. Only
  a read that names another root, shows versions that are not a prefix of
  the known ones (later versions, or a fork where both have a version), or
  is newly unresolved replaces it. A server that leaves rows out of reads
  can still withhold a later version, so that the device reports it as
  invalid until a read shows it; can switch the device to another valid
  root of the address, or another branch of a fork; and, where the device
  holds keys of two roots of one address, so choose which of them it
  writes under. Before its first read in a session, such a device reads
  the chain before it writes (§5.2), so it never writes under a root no
  read of the session showed. A device holding one root writes under it as
  its last read showed. It cannot forge a chain: every root and
  successor is signed and checked (§3.3).
- **Any updater can add roots.** The server does not limit roots per space
  (R15). Roots after the first valid one are ignored by every client (§3.2);
  they cost storage only, and a creator whose root lost repairs its own rows.
- **Equality tokens are linkable.** Tags are per key version, and a query
  sends one token per held version (§4.5), so the server can link the tokens
  of one value across versions, and sees how many rows of a column share a
  value within a version and which values queries look for (but not the
  values).
- **Writes with a held key are not freshness-checked.** A device holding an
  older version's key may write under it until it obtains a newer one or
  Jazz permissions refuse (§5.1).
- **Approving a device trusts its keys unless the codes are compared.** The
  approver sees a requesting device's keys as the server shows them, and a
  new device of an `app` account sees the account's devices as the server
  shows them. Both devices display an approval code (§2.5) built with
  committed nonces; a user who confirms they match defeats a substitution,
  except by a one in a million chance. An approval made without comparing
  them lets a dishonest server approve its own keys under the requesting
  device's ID and receive the account key.
- **App account roots are trusted on first use.** A device account's
  root is signed by the key its account ID derives from, so no one without
  the secret can forge or replace it, or reset the account (§2.3, §2.4),
  once a client has seen that signed root. A client meeting the account for
  the first time cannot tell from the account ID alone that it is
  a device account, so a server can show it an app-kind root instead; from the
  first signed root a client verifies, its floor and the no-downgrade rule
  protect it. An app account has nothing a client can check its root
  against: whoever can write as the account, the server included, can write
  its first root and sealing keys, and a server can show a different root
  (another device and key), or other keys for its root device, to clients
  that have not seen the account before. Every other device's keys are
  signed into its approval (§2.5), so substituting them is detected by
  every client.
  Members delivering to that account then seal to that key. The
  account's own devices raise `root-not-mine` when they see a root other
  than the one they verified (§2.4), but only from their first observation
  on; other accounts have no way to tell. The SDK should present
  `root-not-mine` as "your encryption identity was replaced on the server;
  your data may be exposed", not as a sync error.
- **Rollback is detected only after a first observation.** A server can
  answer a read with an older cut, withholding rows accepted since (a device
  removal, a rotation, a retired recovery root), so that a client acts on
  stale state. Each device keeps account floors (§6.3a) and refuses account
  evidence older than what it has verified (`stale-state`); before its first
  observation of an account, and after a wipe, it cannot tell a rolled-back
  history from the real one. Other devices' floors do not protect it. Spaces
  have no floors: a server can show a device an older state of a space (for
  example, hide a rotation), as it can add a member row.
- **Account reset needs no approval.** For an application that enables
  `allowAccountReset`, whoever can sign in as an app-authenticated account
  can reset it and then receive every space key it is a member of, with the
  full history (§2.11). Members' runtimes emit an `account-reset` event,
  after the fact. The setting is enforced by the server only (R5, R6):
  clients do not check it, so a dishonest server can reset an account the
  application has not allowed to reset, as it can add a member row. The account's
  own devices raise `device-no-longer-active` and must be approved again.
  Device accounts cannot reset: every generation's root must be signed
  by their identity (§2.4 step 1).
- **A second root of one generation ends the account's E2EE.** Only the
  account's own sessions or the server can write one; clients fail closed
  with `conflicting-roots` for good (§2.4 step 1), so whoever can write as
  an app account (or the server) can deny it E2EE service this way, which
  gains it no content.
- **Removing a device of a device account does not exclude it.** A
  device that still holds the account secret is the account (§2.8); the
  remedy for a lost or stolen device of a device account is a new
  account.
- **A stolen active device can remove the owner's other devices.** Any
  active device may remove any other, so whoever holds one can remove the
  owner's devices first and keep the account's new keys. A device cannot
  remove itself, so a removal always comes from a device that stays; a
  removal request it writes only gets it removed by the owner's next
  device, without its ever holding the new key. For an
  app account the remedy is to revoke the stolen device's sign-in and then
  remove it; for a device account, a new account.
- **Device-account recovery is as strong as the account secret.** The E2EE
  recovery key of a device account is derived from its account secret
  (§2.9), so whoever holds that secret (any device that stores it, a script
  on an origin that keeps it in `localStorage`, or anyone with its phrase or
  passkey) can restore the account's E2EE keys, and with them every space key
  sealed to the account. Passkey backups are protected by the platform's
  user verification.
- **Metadata is visible to the server and to sessions permitted by policy:**
  which rows, spaces and devices exist; row IDs, relationships, space
  coordinates, authorship and timing; ciphertext sizes; member rows and key
  records. The public account tables (R10) are readable by every signed-in
  session: anyone can see how many devices an account has enrolled and
  removed, when, and whether recovery is set up. The server stores and
  evaluates every E2EE row, so it sees who is a member of which space and
  who delivers which keys to whom.
- **No side-channel padding.** Ciphertext length reveals plaintext length.
- Local private records are protected only by the platform store; scripts on
  the same origin can read them.
- Integrity of encrypted cells is not checked by the server: Jazz stores any
  bytes a permitted writer submits; readers detect and leave out bad cells.
- **Internal records are never deleted** (R1), except member rows.
  Deliveries grow with members and rotations (one per member per version on
  the server; devices sync those of the current version and held skipped
  versions, and those addressed to their own account, §3.8), and a deleted
  space row or account leaves its records (§1.3). This is accepted for the
  first release; the cost is to be measured with key-version growth.
