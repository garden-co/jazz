import { encodeCryptoContext } from "./context.js";
import { encodeEnvelope } from "./envelope.js";
import { frameCryptoRecord } from "./record-frame.js";
import type { SpaceRoot, SpaceGrant, SpaceDelivery } from "./spaces.js";

type Coordinates = Pick<SpaceRoot, "id" | "scopeId" | "identifier" | "epochId">;
export function spaceContext(
  application: string,
  root: Coordinates,
  column: string,
  recipient = "",
): Uint8Array {
  const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
  if (
    ![root.id, root.scopeId, root.identifier, root.epochId].every(
      (value) => typeof value === "string" && uuid.test(value),
    )
  )
    throw new Error("Invalid E2EE space coordinates");
  return encodeCryptoContext({
    application,
    policy: "jazz.e2ee.space.v1",
    scope: root.scopeId,
    identifier: root.identifier,
    table: "__e2ee_spaces",
    row: root.id,
    column,
    epoch: root.epochId,
    recipient,
  });
}
export function spaceRootBytes(
  application: string,
  root: Omit<SpaceRoot, "signature">,
): Uint8Array {
  return frameCryptoRecord([
    spaceContext(
      application,
      root,
      JSON.stringify([
        "root",
        root.accountId,
        root.deviceId,
        root.accountEpochId,
        root.initialGrantId,
      ]),
    ),
    encodeEnvelope({ id: root.mechanism, version: root.version }, root.verification),
    root.authorEnvelope,
  ]);
}
export function spaceGrantBytes(
  application: string,
  root: Coordinates,
  grant: Omit<SpaceGrant, "signature">,
): Uint8Array {
  if (grant.spaceId !== root.id || grant.epochId !== root.epochId)
    throw new Error("Invalid E2EE space grant coordinates");
  return spaceContext(
    application,
    root,
    JSON.stringify([
      "grant",
      grant.id,
      grant.authorAccountId,
      grant.authorDeviceId,
      grant.authorEpochId,
      grant.operation,
    ]),
    JSON.stringify([grant.recipientKind, grant.recipientId, grant.recipientEpochId]),
  );
}
export function spaceDeliveryContext(
  application: string,
  root: Coordinates,
  delivery: Omit<SpaceDelivery, "signature" | "envelope">,
): Uint8Array {
  if (delivery.spaceId !== root.id || delivery.epochId !== root.epochId)
    throw new Error("Invalid E2EE space delivery coordinates");
  return spaceContext(
    application,
    root,
    JSON.stringify([
      "delivery",
      delivery.id,
      delivery.senderAccountId,
      delivery.senderDeviceId,
      delivery.senderEpochId,
    ]),
    JSON.stringify([
      delivery.recipientAccountId,
      delivery.recipientDeviceId,
      delivery.recipientEpochId,
    ]),
  );
}
export function spaceDeliveryBytes(
  application: string,
  root: Coordinates,
  delivery: Omit<SpaceDelivery, "signature">,
): Uint8Array {
  return frameCryptoRecord([spaceDeliveryContext(application, root, delivery), delivery.envelope]);
}
