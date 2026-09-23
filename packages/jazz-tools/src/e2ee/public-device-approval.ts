import { encodeCryptoContext } from "./context.js";

export type PublicDeviceApproval = Readonly<{
  id: string;
  accountId: string;
  epochId: string;
  deviceId: string;
  signerId: string;
  recoveryRootId?: string | null;
}>;

/** Key-free statement only: the caller must verify possession, eligibility and acceptance. */
export function publicDeviceApprovalBytes(
  application: string,
  approval: PublicDeviceApproval,
): Uint8Array {
  const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
  if (
    ![approval.id, approval.epochId, approval.deviceId, approval.signerId].every(
      (id) => typeof id === "string" && uuid.test(id),
    )
  )
    throw new Error("Invalid E2EE public approval identifiers");
  const recovery = approval.recoveryRootId != null;
  if (recovery && (!uuid.test(approval.recoveryRootId!) || approval.signerId !== approval.deviceId))
    throw new Error("Invalid E2EE recovery approval identifiers");
  return encodeCryptoContext({
    application,
    policy: recovery
      ? "jazz.e2ee.recovery-device-approval.v1"
      : "jazz.e2ee.public-device-approval.v1",
    scope: "account",
    identifier: approval.accountId,
    table: "__e2ee_public_device_approvals",
    row: approval.id,
    column: recovery
      ? `recovery-signature:${approval.recoveryRootId}`
      : `approval-signature:${approval.signerId}`,
    epoch: approval.epochId,
    recipient: approval.deviceId,
  });
}
