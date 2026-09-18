import { encodeCryptoContext } from "./context.js";
import { frameCryptoRecord } from "./record-frame.js";
import type { GroupRecoveryDelivery } from "./groups.js";

/** A recovery root is a recipient, never a device or a membership grant. */
export function groupRecoveryContext(
  application: string,
  record: Omit<GroupRecoveryDelivery, "signature" | "envelope">,
): Uint8Array {
  const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
  if (
    ![
      record.id,
      record.groupId,
      record.epochId,
      record.senderDeviceId,
      record.recoveryRootId,
    ].every((id) => typeof id === "string" && uuid.test(id)) ||
    ![record.senderAccountId, record.recipientAccountId].every(
      (id) => typeof id === "string" && id.length > 0,
    )
  )
    throw new Error("Invalid E2EE group recovery delivery");
  return encodeCryptoContext({
    application,
    policy: "jazz.e2ee.group-recovery.v1",
    scope: "group",
    identifier: record.groupId,
    table: "__e2ee_group_recovery_deliveries",
    row: record.id,
    column: JSON.stringify([record.senderAccountId, record.senderDeviceId]),
    epoch: record.epochId,
    recipient: JSON.stringify([record.recipientAccountId, record.recoveryRootId]),
  });
}

export function groupRecoveryBytes(
  application: string,
  record: Omit<GroupRecoveryDelivery, "signature">,
): Uint8Array {
  return frameCryptoRecord([groupRecoveryContext(application, record), record.envelope]);
}
