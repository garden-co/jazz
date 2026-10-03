# E2EE group recovery deliveries

## Overview

Recovering an account must also permit recovery of its group keys without an
old device remaining online. Device-addressed group envelopes alone cannot
provide that guarantee. Accepted recovery roots are additional recipients for
group keys; they are not devices, group members or Jazz permission grants.

## Details

### Version 1 record and transcript

`__e2ee_group_recovery_deliveries` is an immutable proposal table containing
`id`, `groupId`, `epochId`, `senderAccountId`, `senderDeviceId`,
`recipientAccountId`, `recoveryRootId`, `envelope` and `signature`. Application
policies govern these ordinary Jazz writes and must not allow rewriting or
removing accepted delivery records. The account, device, group and
recovery-root columns use references to their existing managed tables; `epochId`
is a string. This is an additional userland table, not a Jazz wire-protocol
change. Existing device-delivery encodings remain unchanged.

The envelope context is JE2C version 1 with these fields:

| Field       | Value                                               |
| ----------- | --------------------------------------------------- |
| application | The group creator's verified account scope tuple    |
| policy      | `jazz.e2ee.group-recovery.v1`                       |
| scope       | `group`                                             |
| identifier  | `groupId`                                           |
| table       | `__e2ee_group_recovery_deliveries`                  |
| row         | Proposal `id`                                       |
| column      | Compact JSON `[senderAccountId, senderDeviceId]`    |
| epoch       | `epochId`                                           |
| recipient   | Compact JSON `[recipientAccountId, recoveryRootId]` |

The signature transcript contains two `frameCryptoRecord` fields: this context
and the complete envelope bytes, each prefixed by its unsigned 32-bit big-endian
byte length. The proposal, group, epoch, sender-device and recovery-root IDs
must be lowercase UUID v4 values. Account IDs must be non-empty strings.
The byte fixture is framed independently with Python's `struct.pack`, and the
signature test changes each coordinate and the ciphertext independently.

### Required lifecycle integration

Before publishing an envelope, replay accepted group and account history in
the publication transaction. The sender must be an active device of a current
group member. The recipient account must still be a group member, and its
recovery root must be authenticated by accepted account history. A root row or
a valid device signature alone is insufficient. Publish only for an accepted
group epoch after required rotation, subject to ordinary Jazz policy.

On recovery, validate the same historical eligibility at the envelope's
acceptance position, current group membership, the retained recovery material's
binding to its accepted root, and the decrypted group key against the accepted
epoch's verification and predecessor history. Then deliver to the recovered
device through the existing checked group path. Do not retain recovery private
keys in a device store or treat them as device-signing keys.

Recovery registration must cover existing accessible groups; subsequent group
delivery must cover already registered recovery roots, including recipients
whose devices are offline. Rotation must exclude removed accounts' recovery
roots. Interrupted delivery must resume without replacing an accepted epoch.
These are integration requirements, not claims established by the codec test.

### Current group integration

The existing checked group-delivery transaction also seals the current epoch
to each member account's accepted recovery roots. Recovery deliveries share
the historical sender and recipient eligibility checks used by device
deliveries, with an accepted recovery root replacing the recipient-device
check. A signature alone does not confirm the enclosed key.

Recovery creation discovers effective memberships through the same accepted
graph replay used by group loading, inside an authority-settled read. This
includes inherited groups even when their device key has not been delivered.
It loads those groups to publish missing recovery envelopes and then opens the
new recovery path to verify it. A missing
key or denied required publication rejects the operation; already accepted
account recovery records are not rolled back by a later group failure.

Explicit and local-first protected recovery both validate the retained root,
current group membership, delivery eligibility at its acceptance position, and
the decrypted epoch and predecessor chain. The recovered group key is staged
in the existing account-scoped local store. Normal group loading revalidates
membership and performs ordinary-policy-governed device delivery. Recovery
private-key buffers are cleared in `finally` and are not saved in that store.

A recovered staged key must authenticate the accepted epoch and predecessor
history before readiness can tolerate a maintenance write being rejected.
If ordinary policy denies device delivery with `permission_denied`, an otherwise
ready group remains readable from its retained staged key. No denied delivery
row is accepted. If rotation is required, the same denial leaves the group
`maintenance-required`, not `ready`; other errors still propagate.
The read-only recovered-device regression reproduced the missing distinction
as a rejected recovery wait, and now checks readiness alongside the absence of
any accepted delivery to that device.
If recovery stops after staging one of several groups, retrying the same material
can reuse that authenticated key and continue with the remaining groups. A
different retained epoch or key still rejects recovery without being overwritten.

Malformed delivery transcripts are rejected as candidates rather than aborting
the search for a usable envelope. The regression inserts a UUID-v1 proposal
ID through ordinary Jazz policy before a valid recovery delivery; that ID is
valid for Jazz storage but invalid for this E2EE transcript. Previously its
format error aborted recovery creation. The shared delivery validator now
rejects it while retaining the valid envelope. Authority coverage failures and
crypto verification errors remain outside this format-error guard.
This does not qualify repair of correctly signed but unusable ciphertext.

### Inherited recovery protection

The inherited-path regression (`11e8f1dfc4`) first exposed a successful recovery
wait despite an undelivered parent key. A keyless administrator had accepted
the parent-to-child grant, so creator/direct-member/delivery discovery missed
that parent. Validated graph discovery now rejects the recovery wait until a
capable member supplies the key. The retry protects both groups; a fresh device
then restores the child and inherited parent after all old clients shut down.
A required group becoming refused or unavailable during protection rejects the
operation rather than silently shrinking its coverage. Concurrent membership
changes during that process still require dedicated qualification.

### Read-only recovery coverage

After validating account recovery material, `recovery.status(material)` discovers
all current effective group memberships from accepted topology. It checks each
group's recovery delivery at its historical acceptance position and authenticates
the decrypted epoch and predecessor history. It shares those checks with
`recovery.use()` but does not stage keys, enrol devices or publish maintenance.
Local-first protected-material inspection uses the same path.

The result records each group ID and epoch. `validated` is evidence for that
path and the selected recovery root. `unavailable` has one of three reasons:
`missing-recovery-delivery`, `unusable-recovery-delivery` or
`maintenance-required`. A bad opened key cannot become a validated path, and
a missing inherited delivery cannot disappear from the coverage list. Removed
memberships are excluded. Authority/coverage failures reject the inspection;
they are not interpreted as an empty group set or an unusable delivery. This
includes signer failures while replaying predecessor history. Candidate key
opening, unwrapping and key-confirmation failures instead make that delivery
unusable. The key-envelope interface does not distinguish authentication failures
from other rejected adapter reads. A changed account epoch during inspection
requires retry. Status does not guarantee that state remains current after the
reported snapshots or that another registered root was checked.

## Qualification and open questions

This extracted layer has not been executed by its extraction lane. Historical
checkpoint timings are not qualification for this stack. Release qualification,
resource bounds, platform coverage and independent review are tracked in
[the E2EE qualification issue](https://github.com/garden-co/jazz/issues/3125).
