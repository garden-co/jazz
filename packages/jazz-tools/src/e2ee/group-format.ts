import { encodeCryptoContext } from "./context.js";
import { encodeEnvelope } from "./envelope.js";
import type { GroupRoot, GroupDelivery, GroupMembership, GroupRepair } from "./groups.js";
import { frameCryptoRecord } from "./record-frame.js";

/** A recipient may request replacement, never grant itself membership. */
export function groupRepairBytes(
  application: string,
  record: Omit<GroupRepair, "signature">,
): Uint8Array {
  const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
  if (
    ![
      record.id,
      record.groupId,
      record.epochId,
      record.deliveryId,
      record.deviceId,
      record.accountEpochId,
    ].every((id) => typeof id === "string" && uuid.test(id)) ||
    typeof record.accountId !== "string" ||
    !record.accountId
  )
    throw new Error("Invalid E2EE group repair request");
  return encodeCryptoContext({
    application,
    policy: "jazz.e2ee.group-repair.v1",
    scope: "group",
    identifier: record.groupId,
    table: "__e2ee_group_repairs",
    row: record.id,
    column: JSON.stringify([record.deliveryId, record.accountId, record.deviceId]),
    epoch: record.epochId,
    recipient: record.accountEpochId,
  });
}

/** A membership candidate, not authority or permission to disclose keys. */
export function groupMembershipBytes(
  application: string,
  record: Omit<GroupMembership, "signature">,
): Uint8Array {
  const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
  if (
    ![record.id, record.epochId, record.authorDeviceId, record.authorEpochId].every(
      (id) => typeof id === "string" && uuid.test(id),
    ) ||
    !["add", "remove"].includes(record.operation) ||
    !["account", "group"].includes(record.memberKind) ||
    typeof record.authorAccountId !== "string" ||
    !record.authorAccountId ||
    typeof record.memberId !== "string" ||
    !record.memberId
  )
    throw new Error("Invalid E2EE group membership record");
  return encodeCryptoContext({
    application,
    policy: "jazz.e2ee.group-membership.v1",
    scope: "group",
    identifier: record.groupId,
    table: "__e2ee_group_membership",
    row: record.id,
    column: JSON.stringify([
      record.operation,
      record.authorAccountId,
      record.authorDeviceId,
      record.authorEpochId,
    ]),
    epoch: record.epochId,
    recipient: JSON.stringify([record.memberKind, record.memberId]),
  });
}

export function groupContext(
  application: string,
  root: Pick<GroupRoot, "id" | "epochId">,
  column: string,
  recipient = "",
): Uint8Array {
  return encodeCryptoContext({
    application,
    policy: "jazz.e2ee.group.v1",
    scope: "group",
    identifier: root.id,
    table: "__e2ee_groups",
    row: root.id,
    column,
    epoch: root.epochId,
    recipient,
  });
}

export function groupRootBytes(
  application: string,
  root: Omit<GroupRoot, "signature">,
): Uint8Array {
  return frameCryptoRecord([
    groupContext(
      application,
      root,
      JSON.stringify(["root", root.accountId, root.deviceId, root.accountEpochId]),
    ),
    encodeEnvelope({ id: root.mechanism, version: root.version }, root.verification),
  ]);
}

export function groupDeliveryContext(
  application: string,
  root: Pick<GroupRoot, "id" | "epochId">,
  delivery: Omit<GroupDelivery, "signature" | "envelope">,
): Uint8Array {
  return groupContext(
    application,
    root,
    JSON.stringify(["delivery", delivery.id, delivery.senderAccountId, delivery.senderDeviceId]),
    JSON.stringify([delivery.recipientAccountId, delivery.recipientDeviceId]),
  );
}

export function groupDeliveryBytes(
  application: string,
  root: Pick<GroupRoot, "id" | "epochId">,
  delivery: Omit<GroupDelivery, "signature">,
): Uint8Array {
  return frameCryptoRecord([groupDeliveryContext(application, root, delivery), delivery.envelope]);
}
