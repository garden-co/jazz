# Account and device lifecycle formats

Explicit managed-schema and permission composition is required. Recovery record
validation is included for forward safety; recovery authoring is a separate layer.

## Initial account identity, version 1

The first account epoch is represented by one immutable
`__e2ee_account_identities` row whose ID is the authenticated account ID.
Its `deviceId` references the immutable device request, `epochId` is a fresh
UUIDv4 string, and `envelope` is a version-one common envelope containing the
key adapter's sealed 32-byte random epoch secret. These are ordinary Jazz
columns, not a second row serializer. Package policies require verified account
ownership of the identity and a correlated request with the same verified
author account; updates and deletion are denied.

`ledgerVersion` is immutable and equals `1` for new identities. Omission defaults
to `0`, identifying earlier experimental identities. The public account-root
projection carries the same value, enforced by its insert policy's identity
lookup. Neither enrolment nor public membership replay upgrades a version-zero
identity: both refuse it with a migration error. A root publisher cannot promote
an older identity by claiming version one. No legacy migration is implemented.
This version boundary is necessary but does not replace validation that private
lifecycle transitions have their corresponding public authority records.

The seal uses canonical context version one with these exact fields:
`application` is the local store's scope tuple string defined below;
`policy` is the stable package policy identifier `jazz.e2ee.account-identity.v1`
(not a permissions-bundle hash); `scope` is `account`; `identifier` and `row`
are the account ID; `table` is `__e2ee_account_identities`; `column` is
`envelope`; `epoch` is `epochId`; and `recipient` is `deviceId`.
The existing independent context/envelope fixtures pin their byte encodings.
The identity has one accepted creator and is never rewritten for rotation;
successor epochs require separate lifecycle records.

`verification` is a common version-one symmetric key envelope. Its wrapping
key is the same random 32-byte epoch secret; its plaintext is exactly 32 zero
bytes. Its canonical context is the identity context above, except `column`
is `verification` and `recipient` is empty. The mechanism is the configured
key-envelope adapter's exact ID/version, checked by that adapter on opening.
The recipient-independent context lets later approved devices check that a
delivered key matches the accepted identity, without disclosing the key.
They must use the authoritative identity's verification field, not a verifier
supplied alongside a delivery. Authentication failure, a missing verifier,
a wrong-length marker or any non-zero marker byte prevents activation.

This is an offline check on candidate keys and requires the uniformly random
32-byte epoch secret; it must not be reused with a password-derived key. The
BYOC fault-injection regression establishes detection of inconsistent delivery,
not protection from an adapter that deliberately defeats its own checks. This
amends the unpublished initial identity format: earlier experimental identities
without verification are refused, never silently reset or upgraded.

Creation and existing-identity validation use exclusive transactions with
explicit global waits. A matching local device must open the sealed epoch and
authenticate its verification marker before reporting itself active.
Temporary generated/opened secrets and marker buffers are cleared;
other devices remain pending and cannot cause an existing identity to reset.

## Device approval records, version 1

Approval records bind one accepted account epoch; successor records rotate it.
Private proofs accompany the public approval statement without exposing the
handshake to other accounts. Each challenge is a fresh
UUIDv4 addressing one immutable device request. That request fixes the exact
public key and mechanism/version; replacing a key requires a new request ID.
The shared-key authenticators below do not identify a particular approving
device. They must not be reused as proof that a successor was authored by a
remaining device after removal. Independent device signing and the BYOC
`DeviceSigner` adapter are agreed in the
[signature contract](E2EE_CRYPTO_FORMAT.md#built-in-device-signatures-version-1).

The managed records are ordinary account-owned Jazz rows, immutable after
insertion. Challenge inserts must reference a request belonging to the verified
author account; proof, approval and delivery inserts must reference that
account's challenge. These write permissions do not themselves prove possession
of an encryption key. Cryptographic checks are required before trusting rows.

- `__e2ee_device_challenges`: `deviceId`, `epochId`, `envelope`. The envelope
  seals a fresh random 32-byte challenge secret to the requested device key.
- `__e2ee_device_proofs`: `challengeId`, `proof`, `signature`. The proof wraps
  exactly 32 zero bytes under the challenge secret recovered by the pending device.
  Its signature proves possession of that device's enrolled signing key.
- `__e2ee_device_approvals`: `challengeId`, `verification`, `signerId`, `signature`.
  The verification wraps exactly 32 zero bytes under the accepted account epoch
  key. `signerId` references the approving device's immutable request.
- `__e2ee_device_deliveries`: `challengeId`, `envelope`, `verification`. This seals the account
  epoch key to the approved device. Its creation starts only after the approval
  record's explicit global wait succeeds.

Proof, approval and delivery row IDs equal their `challengeId`; consumers reject
mismatched coordinates. The canonical context uses the same application value
as the initial account identity, policy `jazz.e2ee.device-approval.v1`, scope
`account`, identifier equal to the account ID, table
`__e2ee_device_challenges`, row equal to the challenge ID, epoch equal to the
accepted account epoch ID, and recipient equal to the immutable device request
ID. Column is respectively `challenge`, `proof`, `approval` or `delivery`.
Envelopes use the existing common version-one key-envelope encoding. There is
no public plaintext challenge secret.

The signed proof bytes are this canonical context with column `proof-signature`,
followed by the proof envelope's four-byte unsigned big-endian length and exact
bytes. Lengths above `2^32 - 1` are rejected. The signature uses the enrolled
signing mechanism/version and its common signature envelope. The immutable
request ID binds the enrolled public keys; the context binds the application,
account, epoch and fresh challenge, preventing cross-challenge reuse.
The approver verifies both the challenge marker and signature before publishing
approval or delivering the account key. Earlier unsigned experimental proof
rows are not accepted as signed proofs.

The approval signature uses the same canonical context with column exactly
`approval-signature:` followed by `signerId`, then the verification envelope's
four-byte unsigned big-endian length and exact bytes. The signer validates its
signature locally before publishing. Readers verify it against the enrolled
signing public key and mechanism/version. Starting only from the accepted
identity's first device, readers repeatedly accept grants whose signer is already
accepted, whose recipient proof signature is valid, and whose matching delivery
exists. This first traversal only identifies candidate key deliveries; it does
not establish active membership. After opening a candidate and confirming the
accepted epoch, readers repeat the traversal with the recovered key, checking
every link's approval marker and ciphertext-bound delivery verifier before
accepting its recipient as a signer. The final grant must be reachable through
this fully authenticated chain before its key is returned or membership is
reported active. A disconnected or cyclic chain cannot create membership, and
an unauthenticated intermediary cannot authorise a descendant.

This chain applies only to the initial epoch, where membership is additive.
It must not be reused as revocation eligibility: successor records must bind
and validate the exact accepted predecessor and membership revision. Unsigned
experimental grants are not accepted by the signed-grant reader.

Delivery `verification` wraps exactly 32 zero bytes under the account epoch key.
Its authenticated context is the canonical context above with column
`delivery-verification`, followed by the delivery envelope's four-byte unsigned
big-endian byte length and then its exact bytes. Lengths above `2^32 - 1` are
rejected. This binds the ciphertext to an authorised key holder, preventing an
arbitrary delivery row from making a genuine grant appear active. Both existing
key holders and the recipient verify this binding. `active` is authenticated
enrolment with a valid delivery, not an acknowledgement that a remote device
has consumed it; the recipient still verifies the decrypted account key locally.

The approver retains the challenge secret locally, verifies the accepted proof,
then publishes and waits for the key-free approval before sealing a delivery.
The recipient validates a delivery against the accepted identity's key
confirmation and the approval authenticator before activation. Read-only
exclusive acceptance checks prevent optimistic records from establishing trust.
Malformed challenges or unauthenticated grants/deliveries cannot hide a later
valid approval. Concurrent contexts answering for one retained device reconcile
an already accepted matching proof. Replayed proofs fail their challenge context.

The current operation is not a resumable background job: closing its context
cancels a proof wait, and retrying approval starts a fresh challenge. A pending
device must have its E2EE context open to answer. `devices.approve(id)` returns
synchronously; its `wait()` resolves after accepted delivery or rejects on
failure. Session copies of retained device keys are cleared on Db shutdown;
account invalidation already shuts down its Db contexts.

## Account successor records, version 1

`__e2ee_account_successors` is immutable and account-owned. Its row ID is the
independent proposal UUID, not a slot reserved by the predecessor epoch ID.
Columns are `accountId`, `predecessor`, `epochId`, `signerId`, `removedDeviceId`, `membership`,
`revision`, `verification`, `history`, `deliveries` and `signature`.
The epoch and device identifiers are lower-case hyphenated UUIDv4 strings.
A successor cannot reuse its predecessor's epoch ID.

`membership` and `revision` are canonical compact UTF-8 JSON arrays of sorted,
unique UUID strings: remaining device IDs and the approval IDs used to establish
the predecessor membership. `deliveries` is a canonical compact UTF-8 JSON array
of `[deviceId, envelopeBytes]` pairs sorted by device ID. Envelope bytes are
integer arrays in 0–255, with 1–65,536 elements. Its recipients must exactly
match `membership`; the removed device cannot remain in that set. Readers
reject alternate JSON whitespace, duplicate IDs and malformed byte values.
These are local format choices for signed metadata, not a new sync protocol.

The signing prefix uses canonical context version one with application equal
to the verified account scope tuple, policy `jazz.e2ee.account-successor.v1`,
scope `account`, identifier `accountId`, table `__e2ee_account_successors`, row
equal to the proposal ID, epoch equal to the successor, column `signature` and
recipient equal to `signerId`. Append these fields in order, each preceded by
its four-byte unsigned big-endian byte length: UTF-8 `predecessor`, UTF-8 `removedDeviceId`, then
the exact `membership`, `revision`, `verification`, `history` and `deliveries`
bytes. Fields above `2^32 - 1` bytes are rejected. The configured signer signs
the complete record using its versioned signature envelope.

Format validation alone does not authorise a successor. Readers replay the
complete permitted history from a globally accepted covered snapshot, ordered
by authority transaction position. Invalid proposals do not reserve an epoch.
A valid successor must reference the current predecessor, be signed by an
eligible device and include exactly the eligible approvals accepted before its
transaction. A supplied subset is not proof of complete membership.

A key-free approval confers eligibility at its accepted transaction position,
not when its key delivery arrives. Its epoch must have been established by an
earlier authority transaction, including for the first account epoch: pre-epoch
and same-transaction grants cannot become eligible retroactively.
The signer must be eligible before that
transaction; a grant cannot authorise another grant in the same transaction.
Challenges, proofs and referenced requests must exist no later than the grant.
Competing successors for one predecessor in the same transaction are rejected
together, and a successor cannot depend on another successor in that transaction.
These rules avoid using row IDs to invent an intra-transaction order.

Before delivering an account key, the approving client confirms that its grant
remains eligible in the current epoch. A grant accepted by ordinary Jazz policies
after its signer was removed is not a valid cryptographic grant. Adapter errors
must fail the read, not be interpreted as invalid signatures to skip.

The signed frame is pinned by `fixtures/e2ee-account-successor.c`, independently

## Public device approval statement, version 1

The key-free statement contains `id`, `accountId`, `epochId`, `deviceId`
and `signerId`. Record, epoch and device IDs are lower-case hyphenated UUIDv4
strings. Account IDs retain their exact application-scoped representation.
The statement contains no private challenge, proof envelope, key confirmation
or encrypted delivery. Its independent signature does not reuse the private
approval signature, which covers encrypted verification bytes.

Signed bytes are canonical context version one, with application equal to the
verified account scope tuple, policy `jazz.e2ee.public-device-approval.v1`,
scope `account`, identifier `accountId`, table
`__e2ee_public_device_approvals`, row `id`, column
`approval-signature:` followed by `signerId`, epoch `epochId` and recipient
`deviceId`. There are no appended fields. The configured DeviceSigner adds
its versioned signature envelope as usual.

The immutable device ID must resolve to the exact enrolled public keys and
mechanisms. The statement authenticates the approving device's assertion, not
the recipient's private-key possession on its own. Issuance must follow the
private possession checks. Readers must separately validate the signer's
eligibility, complete accepted membership history and the statement's authority
acceptance. Neither a signature nor account-owned insertion alone grants keys.

`fixtures/e2ee-public-device-approval.c` independently pins the frame.
The approver inserts this statement and its signature in the immutable
`__e2ee_public_device_approvals` table in the same transaction as the private
approval. Authenticated application participants can read the public statement,
but not the private approval. Both records share one accepted transaction and
authority position. Publication must settle before key delivery; eligibility
is checked after acceptance. Publication failures propagate through `wait()`
before key delivery. Acceptance alone does not prove signer eligibility.

## Public account successor statement, version 1

`__e2ee_public_account_successors` contains `id`, `accountId`, `predecessor`,
`epochId`, `signerId`, `removedDeviceId`, `membership`, `revision` and `signature`.
IDs other than the account ID are lower-case hyphenated UUIDv4 strings; the
new epoch must differ from its predecessor. Membership uses the canonical sorted,
unique UUIDv4 array defined for private successors and must not contain the removed
device. The public revision is a canonical JSON array of sorted, unique, non-empty
row-ID strings. It preserves raw candidates even when their IDs are not UUIDv4;
otherwise an invalid candidate could prevent a legitimate rotation.

The signature context uses the verified application tuple, policy
`jazz.e2ee.public-account-successor.v1`, scope `account`, identifier `accountId`,
table `__e2ee_public_account_successors`, row `id`, column `signature`, epoch
`epochId` and recipient `signerId`. Append four uint32-big-endian-length-prefixed
fields: UTF-8 predecessor, UTF-8 removed device ID, membership bytes and revision
bytes. `fixtures/e2ee-public-account-successor.c` independently pins this frame.

The public revision contains every observed public approval statement ID for
this account and predecessor epoch, including unverified candidates. It is not
the private revision, which contains eligible private approval IDs. The producer
reads the public revision inside the same exclusive transaction that publishes
both successor records. Both records share a proposal ID and acceptance point.
No private verification, history, proof or delivery bytes are published.

These atomic-pair guarantees describe the producer, not the insert policy:
arbitrary account-authored public rows still require independent verification.
Before consuming a successor, a public reader must establish complete authority
coverage, match its revision against the predecessor's accepted public approval
history, validate signer eligibility, and derive the resulting membership from
valid approvals and the removal. A signature or claimed membership list alone
is insufficient. The public reader validates these requirements before granting eligibility.

## Local device store, version 2

The dedicated `AccountStore` value is JSON text with `format` equal to
`jazz-e2ee-local-devices-v2` and a `devices` array. This is local secret
storage, never a replicated table or an account-selection store. Hosts must
protect it appropriately; this format does not encrypt private keys at rest
or provide protection against a compromised local process.

Each entry has `scope`, `id`, `mechanism`, `version`, `publicKey`, `privateKey`
and `challenge`, followed by `signingMechanism`, `signingVersion`,
`signingPublicKey` and `signingPrivateKey`. Signing fields use the same mechanism
and byte-array constraints; signing and recipient keypairs are independent.
`scope` is the compact JSON string encoding the tuple
`[verified account registry URL, environment, account ID]`, without identifier
normalisation. Scopes must be unique. `id` is a hyphenated UUIDv4. `mechanism`
and `version` follow the common envelope ID/version rules. Each key is a JSON
array of integer bytes (0–255), between 1 and 65,536 elements; the installed
mechanism validates the actual keypair. The challenge is exactly 32 bytes in
the same array representation. Device keypairs and challenges use independent
secure randomness, not the login root.

New records use compact JSON with the fields in the order above; object-key order
and JSON whitespace are not authenticated and readers do not require that
order. No base64, typed-array object encoding, fractional bytes or implicit
string conversion is part of this format. Unknown format versions, duplicate
scopes, invalid fields and key-mechanism changes fail without replacing the
store. Generated records must satisfy those same format constraints and their
recipient keypairs must open a sealed challenge and signing keypairs must pass
a sign/verify check before any durable update. Restored and
concurrently selected keypairs are also checked before publication. A failed
generation check leaves the store untouched and allows a new attempt; it never
replaces a corrupt retained record. Temporary private-key buffers are cleared.
New records are durably selected by `AccountStore.update` before publishing a
pending request; concurrent contexts retain the winner's record.

The unpublished version-one format lacks a signing identity and is rejected
without rewriting it or generating replacement keys. Existing immutable requests
must match both retained public keys and both mechanism/version pairs.
This local consistency check does not authenticate approval records.

`src/e2ee/fixtures/local-device-v2.json` pins a literal record using public test
recipient keys from the independent C fixture and signing keys from RFC 8032.
The integration test substitutes only its ephemeral registry/account coordinates,
restores both keypairs and verifies the store is not rewritten. The original
`local-device-v1.json` fixture pins rejection without migration. These fixture
keys must never protect real data.

## Recovery root signing transcript, version 1

The recovery root binds independent signing and recipient public keys to an
account. It is not an exported device key. This section specifies the transcript;
the managed schema also declares its immutable public record. Root publication
through `recovery.create()` and an initial account-private key delivery are
implemented. Public recipient discovery validates candidate roots against
historical device authority. Recovery use can enrol a fresh device and reopen
that device without another device online. Account rotation also delivers the
next epoch to accepted recovery roots; the registering-device revocation test
covers recovery without another device online after two rotations, including
one initiated by a recovered device.

The signed bytes concatenate three fields, each prefixed by its u32be byte
length, in this order:

1. A version-one `JE2C` context: application is the existing account-context
   scope; policy is `jazz.e2ee.recovery.v1`; scope is `account`; identifier is
   the account ID; table is `__e2ee_recovery_roots`; row is the recovery root ID;
   column is the compact JSON array `["root", signerId]`; epoch is the account
   epoch authorising registration; recipient is empty.
2. A version-one `JE2E` envelope containing the recovery signing public key,
   labelled with its signing mechanism and version.
3. A version-one `JE2E` envelope containing the recovery recipient public key,
   labelled with its key-envelope mechanism and version.

The registering device signs these bytes. Its signature is stored separately.
The codec does not establish authority: replay must establish that the
registering device was eligible at the root's accepted authority position.
Recovery replay uses one globally accepted covered snapshot. Only approvals and
successors strictly before registration establish the registering device's
membership and account epoch. Immutable account-root and device-key projections
remain evidence even when published later; their publication is not activation.
The root's epoch must match that historical epoch, and the registered device's
signature must verify. Malformed candidates are skipped; adapter failures fail
the read. This validation does not itself deliver keys or qualify recovery use.
Ordinary Jazz permission remains required. Later revocation of that device
must not revoke the independent recovery root. Recovery use must validate its
own authority path rather than treating a recovery signature as a device
signature. No private key, recovery material or account key belongs in this
public transcript.

`fixtures/e2ee-recovery-root.c` independently emits the literal transcript pinned
by `recovery-format.test.ts`. The short fixture keys exercise framing only;
they are not valid cryptographic keys or evidence of lifecycle qualification.

### Recovery-backed device approval, version 1

`recovery.use(material)` returns a synchronous handle. Its `wait()` validates
the material against an accepted recovery root, authenticates a current account
epoch delivery and enrols the local device through the existing challenge,
proof, private approval and device-delivery records. An already revoked device
ID cannot be revived; recovery requires a fresh device identity.

The public approval table adds optional `recoveryRootId` and `recoverySignature`
columns. With a recovery root, `signerId` must equal `deviceId`. Both the new
device and the recovery root sign the same version-one `JE2C` context:

- application: the account-context scope;
- policy: `jazz.e2ee.recovery-device-approval.v1`;
- scope: `account`; identifier: the account ID;
- table: `__e2ee_public_device_approvals`; row: the approval ID;
- column: `recovery-signature:<recoveryRootId>`;
- epoch: the approving account epoch; recipient: the new device ID.

`signature` holds the device signature and `recoverySignature` the root
signature. All record, epoch, device and recovery-root IDs are UUIDv4 values.
The literal `recovery-approval-format.test.ts` fixture pins these bytes.
Ordinary approval bytes are unchanged and ordinary approvals must not carry a
recovery signature.

Replay requires the root's accepted registration to precede the approval
strictly. Root registration is validated against the account epoch and active
devices strictly before registration, recursively including earlier valid
recovery approvals. Immutable account-root and device-key projections remain
available as evidence. This permits a recovered device to register a new root
without permitting a root to authorise its own registration.

## Qualification and open questions

This extracted layer has not been executed by its extraction lane. Historical
checkpoint timings are not qualification for this stack. Release qualification,
resource bounds, platform coverage and independent review are tracked in
[the E2EE qualification issue](https://github.com/garden-co/jazz/issues/3125).
