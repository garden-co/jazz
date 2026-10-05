# E2EE group successor epochs

## Overview

An effective removal requires a fresh accepted epoch. Removing a recipient from
the membership view alone does not complete rotation. Remaining members must
receive the replacement key; removed recipients must not receive it.

## Details

The immutable `__e2ee_group_successors` table records the group ID, predecessor
epoch ID, new epoch ID, signing account/device/account-epoch IDs, membership
revision, effective account membership, verification envelope, history envelope,
author self-envelope and device signature. Its row ID independently identifies
the proposal.

The signature transcript is `frameCryptoRecord` over seven fields: JE2C v1
context, UTF-8 predecessor epoch ID, revision bytes, membership bytes,
verification envelope, history envelope and author self-envelope. Context uses policy
`jazz.e2ee.group-successor.v1`, scope `group`, group ID as identifier, table
`__e2ee_group_successors`, proposal ID as row, column `signature`, new epoch ID
as epoch, and compact JSON `[authorAccountId, authorDeviceId, authorEpochId]`
as recipient. Coordinates other than the non-empty account ID are UUID v4.
The predecessor and new epoch must differ.

Revision is canonical JSON containing the sorted, unique IDs of all relevant
accepted membership candidate rows, including invalid candidates. This binds
the authoritative read set, not just the subset that changes effective access.
Membership is canonical JSON of sorted, unique `[accountId, accountEpochId]`
pairs. Account IDs are non-empty; epoch IDs are UUID v4. The format fixture pins
literal transcript bytes, signature binding and canonical metadata rejection.

Signature validity alone does not activate an epoch. Reconciliation must read
the accepted predecessor and complete membership history in an exclusive
transaction, derive the exact effective membership, and verify an active signing
device belonging to a remaining member. A stale revision or competing successor
must not activate. Readers independently validate the same accepted chain.

Generate a fresh 32-byte key. The verification envelope wraps 32 zero bytes under
that key using the ordinary group verification context for the new epoch. The
history envelope wraps the predecessor key under the fresh key with successor
context column `history` and an empty recipient. The author signature binds both
envelopes. The author self-envelope seals the fresh key to the author's existing
device public key, using successor context column `author-envelope` and the
author device ID as recipient. Verify a generated self-envelope by opening it
before submission. It may accompany the proposal because that author already
holds both the fresh and predecessor keys; it discloses nothing new to another
recipient. After acceptance it permits the author to resume delivery without
replacing the existing private group-key staging record. Only then publish other
device envelopes through the ordinary group-delivery path. A failed or interrupted
delivery must preserve the accepted epoch and allow retry.

Generated verification and history envelopes must decrypt to the exact plaintext
supplied to `wrap()` before publication. Apply the same check to initial group
verification before staging its key. Reject malformed ciphertext and valid
ciphertext containing a substituted value; wipe the temporary decrypted copy on
both success and failure. A failed successor preparation does not undo an already
accepted removal request. It must leave no successor row, and a later capable
group load must be able to retry rotation.

This record does not grant membership or Jazz permissions. Applications retain
ordinary successor-insert and delivery policies. Removed members can retain old
keys, but those keys cannot authenticate a successor or obtain its new secret.

`db.e2ee.groups.leave(groupId)` delegates to `remove(groupId, account.id)` and
returns the same synchronous mutation handle. Its `wait()` confirms acceptance
of the removal, not completion of another member's rotation. Ordinary Jazz
policies must permit self-removal; the helper does not bypass them. A departing
account cannot rotate the group after removal. A remaining authorised group
member reconciles the replacement epoch on a subsequent load.

The membership table stores `memberId` as a UUID, so a self-removal policy can
compare it directly with `session.user.account`. Both account and group IDs use
this type; it is not a foreign key to either table. Malformed UUIDs are rejected
at insertion. This changes the managed userland schema, not the Jazz wire
protocol or the signed membership transcript.

### Empty flat-account lineages

When accepted membership becomes empty at a transaction boundary, the group
is sealed. Later membership or successor candidates cannot reactivate it.
`add()` and `remove()` reject a sealed group, and `explain()` reports
`{ state: "refused", reason: "group-sealed" }` before opening any key. Ordinary
administration permission and retention of an old key do not undo sealing.
Creating another group remains possible with a fresh ID and key history.

Membership records sharing one accepted
transaction position are processed before checking whether that position
leaves the lineage empty.

### Nested-group implementation constraints

Keep direct group membership distinct from effective account membership.
Removing an account from a child group must not remove its independent direct
membership in a parent. Conversely, removing the last effective path requires
parent reconciliation and exclusion from the replacement epoch. Child changes
must remain visible when a parent is loaded without an explicit parent mutation.

The settled read set and signed successor revision must cover the relevant
child membership and epoch dependencies, not merely the parent's membership
rows. Historical delivery checks must resolve those dependencies at the
delivery's acceptance position; current child membership cannot retrospectively
authenticate an old delivery. Existing account-device eligibility checks still
apply to every effective account recipient.

Cycle and eight-edge depth validation must include the graph affected by an
addition, including incoming ancestor paths. Read predicates must participate
in exclusive acceptance so competing additions cannot both validate against
an incomplete graph. Signed raw candidates must undergo the same semantic
validation during replay; a valid device signature is not proof of a valid DAG.
Do not recursively trust unvalidated candidate edges to discover recipients.

### Required topology visibility

Agreed: package-defined read policies expose group roots, membership edges
and epoch metadata to every authenticated account in the application.
Applications continue to define administration/write policies. Readability
exposes topology, not plaintext keys, and grants no protected-data access.

`TransactionScope.allSettledForE2ee()` reads the query and matching settlement
metadata through ordinary global reads. Its row/settlement correspondence
check does not prove that application read policies expose every relevant edge.
Existing settlement tests verify covered permitted histories and conflicts,
not completeness across permission-hidden graph records.

Client-side DAG validation must not interpret a permission-filtered graph as
the complete graph. The managed topology read policies must preserve the
agreed visibility; applications cannot narrow these reads while retaining the
same client-side validation contract. This decision resolves the visibility
question; release qualification is tracked below.

`withGroupTopologyPermissions(tables, administration)` now supplies the
package-defined topology reads for explicit managed-schema composition. It
uses the ordinary permissions builder and replaces only the `select` policy
for group roots, membership and successor records. It preserves insert,
update and delete rules, other E2EE relations and ordinary application-table
policies. The read predicate accepts authenticated local-first and external
sessions. Automatic managed-schema installation must reuse this policy
composition; adding the helper does not install policies on an existing app.

Nested successor revisions retain bare membership row IDs, preserving flat
group transcripts. Descendant roots use `__e2ee_groups:<id>` and accepted
descendant successors use `__e2ee_group_successors:<id>`. These namespaced
entries cannot collapse into a membership row with the same UUID. The complete
set remains canonically sorted and covered by the successor signature.

### Eight-edge depth checkpoint

`group-depth.test.ts` constructs nine groups and accepts their eight-edge chain
through the public API. Adding a tenth group's edge below the existing leaf
must fail without inserting a membership row. This requires validation to
include incoming ancestor paths, not just the edited group's descendants.

The owner then publishes that ninth edge as a correctly signed, policy-accepted
raw candidate. Replay leaves the original chain readable by its owner, refuses
the added group's account at both the leaf and the top of the chain, and
publishes no chain-key deliveries to that account. The unrelated self-owned
group's legitimate deliveries are excluded from this assertion.

### Cycle acceptance checkpoint

`group-cycles.test.ts` qualifies the existing two-group cycle guard through
ordinary public APIs and the BYOC signing boundary. It holds two valid opposite
edge proposals before either transaction commits, verifies both reached the
barrier, and requires exactly one successful wait and one accepted edge.
The rejected direction still fails the cycle check on retry. Self-membership
is rejected before any edge is recorded.

The test then publishes the reverse edge as a correctly signed, policy-accepted
raw candidate. The raw row remains readable, but replay must not activate it:
access remains one-way, and the parent member cannot obtain the child key.

### Premature recipient candidates

The current public account-add preflight requires accepted account membership.
A raw account-membership candidate likewise cannot establish effective access
unless its recipient has an accepted E2EE account root strictly before the
candidate's acceptance position. A settled history with no such root makes the
candidate ineligible; it must not abort replay of this or an unrelated group.
Later enrolment does not retroactively activate that candidate. This preserves
the existing add preflight, rather than introducing deferred grants.
Missing authority coverage remains an error and is not treated as an empty
history. The raw candidate remains covered by the signed membership revision.

## Qualification and open questions

This extracted layer has not been executed by its extraction lane. Historical
checkpoint timings are not qualification for this stack. Release qualification,
resource bounds, platform coverage and independent review are tracked in
[the E2EE qualification issue](https://github.com/garden-co/jazz/issues/3125).
