# Independent account recovery

This layer enables account recovery authoring and account-only status inspection.
Group and space coverage are added with their lifecycle implementations.

The `__e2ee_recovery_roots` table stores the transcript inputs as named columns
plus its signature. Ordinary insert policy requires the authenticated author to
own the stated account identity and registering-device request. Update and delete
are denied, including for the owner. Authenticated application participants may
read these key-free records, as they can the device membership ledger. This
exposes the recovery public keys and account linkage, not recovery material.

A row passing this policy is only a candidate. The policy deliberately does not
classify a device as cryptographically eligible or validate its signature. The
recovery-root policy test uses inert bytes to check ownership, public visibility
and immutability; it does not qualify recovery authority or key disclosure.

### Recovery creation and client-only material, version 1

`recovery.create()` returns a synchronous handle. Its `wait()` returns
`{ material }` only after global acceptance of the root and its initial key
delivery. Creation requires an active device and an authenticated account key.
The root's publication transaction checks the paired device history. Newly
generated independent signing and recipient keypairs must pass sign/verify and
seal/open checks before publication. Private byte buffers are cleared afterwards;
the returned string is secret material and cannot be reliably erased by JavaScript.

The material is compact JSON with these fields, in order: `format` (exactly
`jazz-e2ee-recovery-v1`), `scope` (the account-context tuple string), `rootId`,
`mechanism`, `version`, `publicKey`, `privateKey`, `signingMechanism`,
`signingVersion`, `signingPublicKey`, `signingPrivateKey`. Key fields are arrays
of integer bytes, each containing 1–65,536 bytes. Mechanisms identify the adapters
that generated the independent keypairs. The literal `recovery-material.test.ts`
fixture pins the representation; its short keys test framing only. This string
is not encrypted at rest and must never be stored in replicated records or logs.
It contains no device private key and uses no recovery password derivation.

Material import accepts equivalent JSON whitespace and field ordering; the text
is not a signed transcript. It rejects unknown or missing fields, other format
versions, non-UUIDv4 root IDs, a different account-context scope, incompatible
mechanisms and malformed or oversized byte arrays. Input is limited to two
million JavaScript string code units before parsing. Both restored keypairs must
pass seal/open and sign/verify checks. Parsed private-key arrays and failed
imports' private buffers are cleared; a successful import transfers owned private
buffers to the recovery operation, which must clear them afterwards. Importing
material alone does not establish recovery authority: use must match both public
keys to an accepted root and validate its history before enrolling a device.

`__e2ee_recovery_deliveries` has ordinary named columns `rootId`, `epochId` and
`envelope`, plus an independent row ID. The envelope seals the 32-byte account
epoch secret to the recovery recipient key. Its version-one `JE2C` context uses
the account-context application, policy `jazz.e2ee.recovery.v1`, scope `account`,
identifier equal to the account ID, table `__e2ee_recovery_deliveries`, row equal
to the delivery ID, column `envelope`, epoch equal to `epochId` and recipient
equal to `rootId`. Opening this envelope alone does not authenticate an account
epoch. `fixtures/e2ee-recovery-root.c delivery` independently emits the context
pinned in `recovery-material.test.ts`. Recovery use must also validate root
authority and the accepted epoch's key confirmation and history.

Ordinary delivery insert policy requires an accepted recovery root owned by the
authenticated account. Reads are account-private; update and delete are denied.
Because policy lookups do not see the root inserted in the same transaction,
delivery publication follows root acceptance. The envelope stays local until
then. Failure of either wait rejects creation without returning material. An
accepted root can remain after delivery failure; resumable creation and orphan
cleanup are not implemented by this slice. Existing device/account keys are not
replaced.

Account rotation publishes a new delivery for each historically validated
recovery root in the same exclusive transaction as the accepted successor.
The transaction reads the complete account recovery-root predicate and requires
it to match the validated snapshot, so a concurrent registration cannot be
silently omitted. Revoking a registering device does not remove its root from
the recipients. Rotation uses the same delivery context above, with the new
epoch and a fresh delivery ID. This differs from initial root creation's
two-step publication: every rotation recipient root already exists.

The public and private approvals must be accepted at the same authority
position. Recovery permits the private approval's signer to be its recipient
only when paired with an accepted recovery-backed public approval; all existing
challenge, proof, signature and account-key checks still apply. Generated
private and public signatures are verified before publication. The client waits
for approval acceptance, opens and checks its own final device envelope, and
waits for delivery acceptance before successful completion. Faulty BYOC output
must reject rather than publish an invalid private signature or device envelope.

### Local-first recovery material protection, version 1

The existing `CellCipher` symmetric adapter protects the UTF-8 recovery material
using the 32-byte random local-first account secret. The built-in cipher derives
its context-specific encryption key; no password derivation or device/recovery
keypair derivation is introduced. External JWTs are not protection secrets.
The adapter's ordinary versioned ciphertext envelope is stored unchanged.

The `JE2C` context uses the account-context application, policy
`jazz.e2ee.local-recovery-protection.v1`, scope `account`, the account ID as
identifier, table `__e2ee_recovery_protectors`, column `material`, and the immutable
recovery-root ID as row, epoch and recipient. This root-lifetime context does not
change with account-key rotation. `recovery-protection.test.ts` pins the literal
context independently and checks tampering, wrong secrets and cross-context
substitution with native crypto.

The helper limits material to two million JavaScript string code units and
decodes authenticated plaintext as strict UTF-8. Temporary plaintext byte
buffers are cleared; callers own and must clear secret buffers. Returned secret
strings cannot be reliably erased by JavaScript. Opening this wrapper does not
establish root authority: the normal material and history checks still apply.
The managed `__e2ee_recovery_protectors` table contains `rootId` and encrypted
`material` columns. Reads are restricted to the creating account, inserts must
refer to that account's existing recovery root, and updates/deletes are denied.
Local-first `recovery.create()` verifies and publishes this protector after
root and delivery acceptance, and waits globally before returning material.
External-auth creation returns explicit material without deriving a protector
from the JWT. Protector-publication failure rejects creation but can leave the
accepted root and delivery; resumable publication remains unfinished.

`recovery.use()` without material explicitly opens account-private protectors
with the retained local-first account secret, checks the embedded root ID and
runs the ordinary recovery-material and historical-authority checks. It tries
later candidates after a failed candidate and reports failure if none works.
It does not activate a device automatically during sign-in. External-auth
accounts require explicit material or another active device's approval.

## Qualification and open questions

This extracted layer has not been executed by its extraction lane. Historical
checkpoint timings are not qualification for this stack. Release qualification,
resource bounds, platform coverage and independent review are tracked in
[the E2EE qualification issue](https://github.com/garden-co/jazz/issues/3125).
