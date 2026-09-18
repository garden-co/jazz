# E2EE group delivery repair

## Overview

A valid device signature proves authorship, not that the recipient can open
the enclosed key. A recipient that has no usable delivery requests a replacement
through the group lifecycle. This does not add a public method or change membership.

## Details

The immutable `__e2ee_group_repairs` table contains `groupId`, `epochId`,
`deliveryId`, `accountId`, `deviceId`, `accountEpochId` and `signature`, plus its
ordinary row ID. Application policy permits authenticated participants to read
requests and an account to insert its own requests. No update or deletion is
needed. Request insertion does not require membership-administration or
key-delivery permission; validation below is independent of that policy.

A request is signed by the recipient device. The signed bytes use JE2C v1 from
[the crypto format](E2EE_CRYPTO_FORMAT.md), with policy
`jazz.e2ee.group-repair.v1`, scope `group`, the group ID as identifier, table
`__e2ee_group_repairs`, and the request ID as row. Column is compact JSON
`[deliveryId, accountId, deviceId]`; epoch is `epochId`; recipient is
`accountEpochId`. All IDs except the account ID are UUID v4. The account ID is
non-empty. `group-repair-format.test.ts` pins literal bytes and signature binding.

Only a globally accepted request from the delivery's recipient is eligible.
Its device must have been active, in the stated account epoch, and an effective
group member at request acceptance. The referenced delivery must precede the
request and authenticate against its own accepted history. Verify the request
signature against that device's accepted public key. Invalid or unrelated
requests must not suppress another recipient's delivery.

A request invalidates only the referenced delivery for deduplication. A later
usable delivery remains eligible. Repeated reads must reuse an accepted request
for the same failed delivery, not produce an unbounded series of duplicates.
Before sending a replacement, the capable client revalidates current group and
device membership in the ordinary exclusive delivery transaction. Ordinary Jazz
policy still controls whether that client may publish the replacement.

No request contains a secret or proves inability to decrypt. An authorised
recipient can deliberately request another envelope; it cannot use a request
to disclose a key to a different recipient or revive revoked membership.

## Qualification and open questions

This extracted layer has not been executed by its extraction lane. Historical
checkpoint timings are not qualification for this stack. Release qualification,
resource bounds, platform coverage and independent review are tracked in
[the E2EE qualification issue](https://github.com/garden-co/jazz/issues/3125).

Delivery-request retention, compaction and repeated-request rate limits are also
tracked there; they never replace signature, membership or acceptance checks.
