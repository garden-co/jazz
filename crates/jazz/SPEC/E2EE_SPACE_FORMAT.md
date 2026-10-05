# Scoped space lifecycle formats

This layer exposes explicit lifecycle and atomic preparation, not automatic encrypted
ordinary operations. Every history decision completes an authority-settled read; no
durable accepted-history cache exists yet.

## Deterministic root identity, version 1

Before signing an initial root, encode the exact compact JSON array
`["jazz.e2ee.space-id.v1", scopeId, identifier]` as UTF-8, without whitespace or
normalisation. Both IDs have already been validated as canonical lowercase UUIDs.
Hash with SHA-256, retain the first 16 bytes, set byte 6 to `(byte & 0x0f) | 0x80`
and byte 8 to `(byte & 0x3f) | 0x80`, and render the bytes as a lowercase
hyphenated UUID. The UUID-v8 value names the root for this portable scope/row pair.
The random epoch and signed initial grant remain independent IDs.
Replay recomputes this identity before accepting a root. A different ID is invalid
even when its signature and initial grant are otherwise consistent.

Independent Python `hashlib` byte corpus (not a runtime qualification receipt):

- Scope: `77777777-7777-4777-8777-777777777777`
- Identifier: `88888888-8888-4888-8888-888888888888`
- UTF-8 preimage: `["jazz.e2ee.space-id.v1","77777777-7777-4777-8777-777777777777","88888888-8888-4888-8888-888888888888"]`
- SHA-256: `73726102dec3e588961444da93affdfa409fc111e3085c4414b0741695cdb505`
- Root ID: `73726102-dec3-8588-9614-44da93affdfa`

The initialising transaction reads the exact root ID including deleted rows at
global durability, then uses exclusive upsert to record an exact-row precondition.
A permission-filtered absent query must not produce a second accepted root; hidden,
deleted and concurrent roots do not permit a fresh lineage.

## Initial space lifecycle transcripts

Space transcripts use `JE2C` v1 with policy `jazz.e2ee.space.v1`, the portable
table-lineage UUID as scope, the scope-row UUID as identifier, `__e2ee_spaces`
as table, the root UUID as row, and the space epoch UUID. These coordinates
must be canonical lower-case UUID strings. The application context follows the
existing account-scoped lifecycle convention for the root author.

The root's column is compact JSON
`["root", accountId, deviceId, accountEpochId, initialGrantId]`; recipient is
empty. Its signed record frames three fields with u32be lengths: that context,
the `JE2E`-encoded verification value and the creating device's sealed key
envelope. The latter uses column `author` and device ID as recipient. It is
encrypted to a device that already holds the new secret; its presence does not
make the root accepted. Verification wraps 32 zero bytes under the space key
with column `verification` and an empty recipient.

A grant signs a context whose column is
`["grant", id, authorAccountId, authorDeviceId, authorEpochId, operation]` and
recipient is `[recipientKind, recipientId, recipientEpochId]`. A delivery uses
column `["delivery", id, senderAccountId, senderDeviceId, senderEpochId]` and
recipient `[recipientAccountId, recipientDeviceId, recipientEpochId]`. Its
signature covers a length-framed context followed by its sealed envelope;
the same context binds the envelope. Grant/delivery space and epoch IDs must
match the root used to construct the context.

The root and its named initial grant must have the same accepted
authority position. Their author coordinates match, but the recipient need
not be the author. Author and recipient histories are replayed strictly before
that position; the grant pins the recipient's accepted account or group epoch.
Initial group grants require an existing, unsealed group with no outstanding
rotation strictly before that position. Their effective recipients come from
the same authenticated graph replay as later group grants, not the initialiser.
This extends initial selection without changing the signed field layout.
Earlier self-grant-only readers reject non-creator initial grants, and earlier
account-only readers reject initial group grants.
Delivery requires a later accepted position and active
sender/recipient devices in the preceding authenticated account history.
The exact root author/device/account epoch may deliver the original space
epoch without being a recipient. This is the initial hand-off of a key that
device already generated, not membership or authority over successor epochs.
All recipients still require current accepted membership, and stale membership
prevents delivery. Other senders must be current recipients.
Account-epoch changes require maintenance; they cannot silently reuse the
initial grant. Subsequent account grants reuse this transcript and pin the
recipient's account epoch at the accepted grant position. Their author and
recipient histories must precede that position; later enrolment cannot validate
an earlier candidate. Delivery replay reconstructs membership strictly before
the delivery position and checks both accounts' active device sets and epochs.
Group grants use the same transcript with `recipientKind` equal to `group` and
`recipientEpochId` equal to the group's accepted key epoch. The group must exist
in authenticated graph history strictly before the grant, and an addition must
not target a sealed group or one requiring rotation. Public ID inference rejects
an ID that resolves to both an account and a group rather than guessing.
Delivery expands accepted group membership to accounts and their approved
devices; it does not add a different envelope encoding. Historical delivery
validation replays group membership strictly before the delivery position.
Relevant group roots, membership records, successors and account histories are
covered by the same enclosing exclusive transaction as the space records.
An explicit account grant is retained separately from inherited group access.
Stale/sealed groups or changed pinned epochs require maintenance. Space
successors are described below. Space inspection attempts one eligible group
reconciliation pass, then re-reads accepted state before space rotation.
Explicit recipient sets are de-duplicated and published atomically with the root;
one denied or ineligible recipient rejects the entire initial transaction.
`fixtures/e2ee-space.c` independently emits the three initial
transcripts; the TypeScript format test compares literals and checks tampering.

## Space recovery delivery transcript, version 1

`__e2ee_space_recovery_deliveries` contains `id`, `spaceId`, `epochId`,
`senderAccountId`, `senderDeviceId`, `senderEpochId`, `recipientAccountId`,
`recipientEpochId`, `recoveryRootId`, `envelope` and `signature`. It is a
managed Jazz table, not a core wire-message change. A recovery root is a
separate recipient, never a device ID or a membership grant.

The transcript reuses the space's `JE2C` v1 context and policy
`jazz.e2ee.space.v1`. Scope, identifier, table and row remain the space-root
coordinates; epoch is the delivered space epoch. Column is compact JSON
`["recovery-delivery", id, senderAccountId, senderDeviceId, senderEpochId]`.
Recipient is compact JSON
`[recipientAccountId, recoveryRootId, recipientEpochId]`. All UUID coordinates
must be canonical lower-case strings; account IDs must be non-empty strings.
Record space and epoch must match the supplied space coordinates.

The envelope seals the space key to the recovery root's recipient public key
with that context. The signature covers two u32be-length-prefixed fields:
the context, then the exact envelope bytes. The distinct column role prevents
reinterpretation as a device delivery. `fixtures/e2ee-space-recovery.c`
independently emits the pinned frame; the format test checks each coordinate,
application, envelope and cross-role substitution.

Creation and use consume this format.
Delivery uses the ordinary exclusive space snapshot and waits for global
acceptance. Only accepted recovery roots of current recipient accounts receive
envelopes. Replay reconstructs space membership and account histories strictly
before the delivery's own table-qualified settlement position, including the
sender's active device, both account epochs and the registered recovery root.
The original-key initial-author exception is the same as for device delivery;
it does not extend to successor keys. A valid signature alone is insufficient.

Recovery creation backfills existing effective space memberships and opens
their recovery envelopes before returning. Recovery use matches the material's
public keys and mechanisms to an accepted root, then confirms the current
space key and its complete predecessor chain. It excludes removed memberships
and sealed spaces, and rechecks current state before delivery or rotation.
Private recovery keys are cleared afterwards and are not saved locally.

### Locally recovered space keys, version 1

The version-two local device store may contain `recoveredSpaceKeysV1`: an
array of `{ scope, spaceId, epochId, payload }` objects, with unique
`(scope, spaceId, epochId)` tuples. Scope is the recovering account-context
string; space and epoch are canonical lower-case UUIDs. Payload contains
exactly 32 integer bytes in 0–255. Other device-store fields are preserved.
Repeated retention of the same key is idempotent; a different key for an
existing tuple is rejected. The literal `local-space-recovery-v1.json` fixture
pins the representation and uses public test key bytes.

These are private candidate keys, not authority. Every load requires current
active-device and space membership checks and authenticates the accepted
epoch's complete key history. Local retention allows a recovered read-only
device to reopen without publishing a device delivery; Jazz still controls
those writes. This storage has the same at-rest limits as the device store,
not additional encryption or guaranteed VM erasure. Historical entries are
retained; pruning is not implemented. Broader concurrency, revocation and
platform qualification are tracked below.

Read-only recovery status shares the recovery-root and key-history checks
without loading a device or retaining any key. It discovers current required
spaces through accepted membership, checks the account epoch against the
enclosing status result and distinguishes missing, unusable and maintenance
paths. Status never repairs a missing delivery or rotates a stale epoch.
Its results describe the accepted per-path snapshots, not one atomic snapshot
of all spaces or a guarantee against later changes.

Recovery and device-delivery selection can skip a candidate that cannot be opened
or whose current key cannot be confirmed. Once that key is confirmed, failures
while replaying membership or processing predecessor history propagate to the
caller; they are not reported as a missing or unusable delivery.

## Space successor transcript

`__e2ee_space_successors` holds immutable UUID `id`, `spaceId`, `predecessor`,
`epochId`, `authorAccountId`, `authorDeviceId` and `authorEpochId`, plus byte
columns `revision`, `membership`, `verification`, `history`, `authorEnvelope`
and `signature`. These are ordinary managed Jazz rows, not a new core wire
message. Predecessor and successor epoch IDs must differ; replay rejects epoch
reuse and proposals that do not name the accepted predecessor.

The transcript reuses `JE2C` v1 and policy `jazz.e2ee.space.v1`. Scope,
identifier, table (`__e2ee_spaces`) and row remain the original space's
coordinates. Epoch is the proposed successor epoch. Column is compact JSON
`["successor", proposalId, role]`; this binds the proposal's own ID despite the
context row remaining the space ID. The signature role is `signature`, with
recipient `[authorAccountId, authorDeviceId, authorEpochId]` as compact JSON.
The signed bytes are seven length-framed fields, in this order:

1. That context.
2. The predecessor UUID as UTF-8.
3. The canonical revision bytes.
4. The canonical membership bytes.
5. Verification ciphertext.
6. Historical-key ciphertext.
7. Author-envelope ciphertext.

Revision reuses the sorted, unique, compact UTF-8 JSON string-array encoding
of `encodePublicApprovalRevision`. Every eligible space grant contributes
`space-grant:<id>`, including the named initial grant. Each currently granted
group contributes `group:` followed by compact JSON
`[groupId, acceptedGroupEpochId, sealedBoolean, groupRevisionArray]`; the final
array is decoded from the existing group's canonical revision. Membership
reuses the canonical account/epoch pair-list encoding of `encodeGroupMembership`.
Replay recomputes both values from the covered histories strictly before the
proposal position, rather than trusting the signed values alone.

Verification wraps 32 zero bytes under the new key using the ordinary space
verification context at its new epoch. History wraps the predecessor's 32-byte
key under the new key, with successor role `history` and empty recipient.
The author envelope seals the new key to the author's already-approved device,
using role `author-envelope` and that device ID as recipient. No other recipient
envelope is produced until authoritative acceptance. Readers confirm each epoch
and unwrap backward through the accepted chain to the signed initial root;
successful decryption of only the latest verification value is insufficient.

The author must be an eligible remaining account with an active device and
the current accepted account epoch. A successor must respond to required
maintenance; stale group state must first be reconciled. Accepted removal and
successor events are replayed by authority position, not local arrival order.
Subsequent grants and deliveries bind the accepted current space epoch.
`fixtures/e2ee-space-successor.c` independently emits the pinned signature
transcript. Its matching TypeScript test also checks transplanted coordinates,
author, predecessor, revision, membership and ciphertext bytes. Adversarial
acceptance, repeated-history and race qualification are tracked below.

## Qualification and open questions

This extracted layer has not been executed by its extraction lane. Historical
checkpoint timings are not qualification for this stack. Release qualification,
resource bounds, platform coverage and independent review are tracked in
[the E2EE qualification issue](https://github.com/garden-co/jazz/issues/3125).
