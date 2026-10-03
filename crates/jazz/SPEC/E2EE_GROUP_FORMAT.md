# Group lifecycle formats

## Initial group records, version 1

`__e2ee_groups` holds an immutable initial root: `id`, `accountId`, `deviceId`,
`accountEpochId`, `epochId`, `mechanism`, `version`, `verification` and `signature`.
The group ID and initial epoch ID are independent fresh UUIDv4 strings. The
creator generates a random 32-byte symmetric epoch secret; no extra group
asymmetric keypair is needed. `verification` wraps 32 zero bytes under that
secret, using the configured key-envelope adapter.

Group contexts use canonical context version one. `application` is the creator's
verified account scope tuple, `policy` is `jazz.e2ee.group.v1`, `scope` is `group`,
`identifier` and `row` are the group ID, `table` is `__e2ee_groups`, and `epoch`
is the group epoch ID. The verification context has `column` equal to
`verification` and an empty recipient. For the root signature, `column` is
compact JSON `["root", accountId, deviceId, accountEpochId]`, with an empty
recipient. Sign two uint32-big-endian-length-prefixed byte fields: this context,
then a common envelope whose mechanism/version are the root fields and whose
payload is the exact `verification` bytes. This outer envelope binds the root's
mechanism declaration; it does not encrypt the verification envelope again.

`__e2ee_group_deliveries` holds immutable `id`, `groupId`, `epochId`,
`senderAccountId`, `senderDeviceId`, `recipientAccountId`, `recipientDeviceId`,
`envelope` and `signature`. Its context uses the root coordinates above, with
`column` equal to compact JSON
`["delivery", id, senderAccountId, senderDeviceId]` and `recipient` equal to
compact JSON `[recipientAccountId, recipientDeviceId]`. Seal the 32-byte group
secret to the recipient's enrolled public key with that context. Sign two
uint32-big-endian-length-prefixed fields: the context and exact envelope bytes.
Readers separately require delivery `groupId` and `epochId` to match the root.
The configured DeviceSigner adds its versioned signature envelope as usual.

The initial implementation grants the creator account only. Root acceptance and
subsequent delivery each use an exclusive transaction containing paired public
and private device-history validation, followed by an explicit global wait.
A preflight membership check alone is insufficient. Readers validate the
creator's eligibility before root acceptance and the sender and recipient's
eligibility before delivery acceptance. A delivery must be accepted strictly
after the root. The signed root's account epoch must still be current.
Signatures alone do not establish any of these conditions. Applications import
`groupSchema` and `withGroupTopologyPermissions` from `jazz-tools/e2ee` to compose
the group tables and public topology reads with their own administration policies.

Group roots, membership changes and successors require an accepted author account
root strictly before the candidate's acceptance position. A covered history with
no eligible root makes the candidate ineligible; later enrolment cannot give it
retroactive authority. Missing settlement coverage still rejects graph loading.

`fixtures/e2ee-group.c` independently emits the root and delivery frames used
by the literal codec test. These fixtures pin framing, not membership validation.

### Local group-key staging, version 1

The local device-store object may contain `stagedGroupKeysV1`, an array of
`{ scope, groupId, epochId, payload }` objects. `payload` is exactly 32 integer
bytes in 0–255. Each `(scope, groupId)` pair is unique. Other device-store fields
are preserved. The creator stages its secret before proposing the root and
removes it after globally accepted delivery. This is private write-ahead state,
not accepted membership or proof that creation succeeded. It shares the local
store's at-rest protection limits.

Loading a group through `explain({ groupId })` now attempts to finish an
interrupted initial delivery when this account's local store has a matching
staged entry. It does not recreate the root or choose a new epoch. The delivery
transaction revalidates the accepted root and paired device history, requires
the staged epoch to match, and authenticates the candidate key against the
root's verification marker before sealing anything. Its global wait must
succeed before staging is removed or readiness is reported. Corrupt candidates
are not replaced with fresh keys; failed attempts retain staging for retry.
Recovery may stage the same epoch and identical 32-byte key again. The atomic
store update leaves an exact match unchanged and rejects a different key or epoch.
This makes interrupted recovery retryable without overwriting a retained secret.
Before publishing, both creation and resumption open the envelope generated
for the sending device and check that it recovers the same key. This prevents
a faulty adapter's unusable output from discarding the only retained secret.
It does not independently validate envelopes addressed to other devices;
each recipient must still open and confirm its own key.
Staging is not consulted by ordinary plaintext or device administration calls.

This does not yet resume an absent/unaccepted root, reconcile a stale account
epoch, or clean up rejected proposals. Multiple contexts may publish equivalent
deliveries if both resume before local cleanup; those envelopes carry the same
accepted epoch, not competing initialisations.

`src/e2ee/fixtures/local-group-staging-v1.json` pins a literal staged entry.
The lifecycle check restores it before device enrolment, creates a different
group and verifies that only the new group's staging is removed after delivery.
This checks preservation and the durable representation, not resumed delivery.
The fixture key is public test data and must never protect real data.

### Group membership candidate signatures (v1)

The managed `__e2ee_group_membership` table records signed membership candidates.
A signature alone does not authorise an operation or disclose a group key;
accepted-history replay and ordinary Jazz policies must establish authority.

The signed bytes are a `JE2C` v1 context with the application context, policy
`jazz.e2ee.group-membership.v1`, scope `group`, group ID as identifier,
table `__e2ee_group_membership`, and the candidate ID as row. The column is the
compact JSON array `[operation, authorAccountId, authorDeviceId, authorEpochId]`.
The epoch is the group epoch ID; recipient is `[memberKind, memberId]`, also
compact JSON. Operations are `add` or `remove`; member kinds are `account` or
`group`. Candidate, epoch, author-device and author-epoch IDs are UUID v4.
Account and member IDs must be non-empty; context field limits also apply.
The record stores these fields plus the device signature. Public API member
type inference is separate from this explicit signed representation.

`group-membership-format.test.ts` pins independently framed literal bytes and
checks signature failure after changes to every coordinate, operation or
recipient. This format test does not qualify membership acceptance or delivery.

## Qualification and open questions

This extracted layer has not been executed by its extraction lane. Historical
checkpoint timings are not qualification for this stack. Release qualification,
resource bounds, platform coverage and independent review are tracked in
[the E2EE qualification issue](https://github.com/garden-co/jazz/issues/3125).
