import { spaceContext } from "./space-format.js";
import { frameCryptoRecord } from "./record-frame.js";
import type { SpaceRoot, SpaceRecoveryDelivery } from "./spaces.js";

type Coordinates = Pick<SpaceRoot, "id" | "scopeId" | "identifier" | "epochId">;

/** Recovery roots are recipients, not devices or new membership grants. */
export function spaceRecoveryContext(
  application: string,
  root: Coordinates,
  delivery: Omit<SpaceRecoveryDelivery, "signature" | "envelope">,
): Uint8Array {
  const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
  if (
    delivery.spaceId !== root.id ||
    delivery.epochId !== root.epochId ||
    ![
      delivery.id,
      delivery.senderDeviceId,
      delivery.senderEpochId,
      delivery.recipientEpochId,
      delivery.recoveryRootId,
    ].every((id) => typeof id === "string" && uuid.test(id)) ||
    ![delivery.senderAccountId, delivery.recipientAccountId].every(
      (id) => typeof id === "string" && id.length > 0,
    )
  )
    throw new Error("Invalid E2EE space recovery delivery coordinates");
  return spaceContext(
    application,
    root,
    JSON.stringify([
      "recovery-delivery",
      delivery.id,
      delivery.senderAccountId,
      delivery.senderDeviceId,
      delivery.senderEpochId,
    ]),
    JSON.stringify([
      delivery.recipientAccountId,
      delivery.recoveryRootId,
      delivery.recipientEpochId,
    ]),
  );
}

export function spaceRecoveryBytes(
  application: string,
  root: Coordinates,
  delivery: Omit<SpaceRecoveryDelivery, "signature">,
): Uint8Array {
  return frameCryptoRecord([spaceRecoveryContext(application, root, delivery), delivery.envelope]);
}
