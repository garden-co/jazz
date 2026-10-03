import { expect, it } from "vitest";
import { createBrowserDeviceSigner } from "./browser.js";
import {
  spaceContext,
  spaceRootBytes,
  spaceGrantBytes,
  spaceDeliveryBytes,
} from "./space-format.js";

const root = {
  id: "11111111-1111-4111-8111-111111111111",
  epochId: "22222222-2222-4222-8222-222222222222",
  deviceId: "33333333-3333-4333-8333-333333333333",
  accountEpochId: "44444444-4444-4444-8444-444444444444",
  initialGrantId: "55555555-5555-4555-8555-555555555555",
  scopeId: "77777777-7777-4777-8777-777777777777",
  identifier: "88888888-8888-4888-8888-888888888888",
  accountId: "account-id",
  mechanism: "test",
  version: 1,
  verification: new Uint8Array([0xaa]),
  authorEnvelope: new Uint8Array([0xbb, 0xcc]),
};
const grant = {
  id: root.initialGrantId,
  spaceId: root.id,
  epochId: root.epochId,
  authorAccountId: root.accountId,
  authorDeviceId: root.deviceId,
  authorEpochId: root.accountEpochId,
  operation: "add",
  recipientKind: "account",
  recipientId: root.accountId,
  recipientEpochId: root.accountEpochId,
};
const delivery = {
  id: "66666666-6666-4666-8666-666666666666",
  spaceId: root.id,
  epochId: root.epochId,
  senderAccountId: root.accountId,
  senderDeviceId: root.deviceId,
  senderEpochId: root.accountEpochId,
  recipientAccountId: root.accountId,
  recipientDeviceId: "99999999-9999-4999-8999-999999999999",
  recipientEpochId: root.accountEpochId,
  envelope: new Uint8Array([0xbb, 0xcc]),
};

// Independently emitted by SPEC/fixtures/e2ee-space.c, not these serializers.
it("matches independent root, grant and delivery transcripts", () => {
  const expected = [
    "AAABaUpFMkMBAAAAB2ZpeHR1cmUAAAASamF6ei5lMmVlLnNwYWNlLnYxAAAAJDc3Nzc3Nzc3LTc3NzctNDc3Ny04Nzc3LTc3Nzc3Nzc3Nzc3NwAAACQ4ODg4ODg4OC04ODg4LTQ4ODgtODg4OC04ODg4ODg4ODg4ODgAAAANX19lMmVlX3NwYWNlcwAAACQxMTExMTExMS0xMTExLTQxMTEtODExMS0xMTExMTExMTExMTEAAACKWyJyb290IiwiYWNjb3VudC1pZCIsIjMzMzMzMzMzLTMzMzMtNDMzMy04MzMzLTMzMzMzMzMzMzMzMyIsIjQ0NDQ0NDQ0LTQ0NDQtNDQ0NC04NDQ0LTQ0NDQ0NDQ0NDQ0NCIsIjU1NTU1NTU1LTU1NTUtNDU1NS04NTU1LTU1NTU1NTU1NTU1NSJdAAAAJDIyMjIyMjIyLTIyMjItNDIyMi04MjIyLTIyMjIyMjIyMjIyMgAAAAAAAAAPSkUyRQEEdGVzdAAAAAGqAAAAArvM",
    "SkUyQwEAAAAHZml4dHVyZQAAABJqYXp6LmUyZWUuc3BhY2UudjEAAAAkNzc3Nzc3NzctNzc3Ny00Nzc3LTg3NzctNzc3Nzc3Nzc3Nzc3AAAAJDg4ODg4ODg4LTg4ODgtNDg4OC04ODg4LTg4ODg4ODg4ODg4OAAAAA1fX2UyZWVfc3BhY2VzAAAAJDExMTExMTExLTExMTEtNDExMS04MTExLTExMTExMTExMTExMQAAAJFbImdyYW50IiwiNTU1NTU1NTUtNTU1NS00NTU1LTg1NTUtNTU1NTU1NTU1NTU1IiwiYWNjb3VudC1pZCIsIjMzMzMzMzMzLTMzMzMtNDMzMy04MzMzLTMzMzMzMzMzMzMzMyIsIjQ0NDQ0NDQ0LTQ0NDQtNDQ0NC04NDQ0LTQ0NDQ0NDQ0NDQ0NCIsImFkZCJdAAAAJDIyMjIyMjIyLTIyMjItNDIyMi04MjIyLTIyMjIyMjIyMjIyMgAAAD9bImFjY291bnQiLCJhY2NvdW50LWlkIiwiNDQ0NDQ0NDQtNDQ0NC00NDQ0LTg0NDQtNDQ0NDQ0NDQ0NDQ0Il0=",
    "AAAByUpFMkMBAAAAB2ZpeHR1cmUAAAASamF6ei5lMmVlLnNwYWNlLnYxAAAAJDc3Nzc3Nzc3LTc3NzctNDc3Ny04Nzc3LTc3Nzc3Nzc3Nzc3NwAAACQ4ODg4ODg4OC04ODg4LTQ4ODgtODg4OC04ODg4ODg4ODg4ODgAAAANX19lMmVlX3NwYWNlcwAAACQxMTExMTExMS0xMTExLTQxMTEtODExMS0xMTExMTExMTExMTEAAACOWyJkZWxpdmVyeSIsIjY2NjY2NjY2LTY2NjYtNDY2Ni04NjY2LTY2NjY2NjY2NjY2NiIsImFjY291bnQtaWQiLCIzMzMzMzMzMy0zMzMzLTQzMzMtODMzMy0zMzMzMzMzMzMzMzMiLCI0NDQ0NDQ0NC00NDQ0LTQ0NDQtODQ0NC00NDQ0NDQ0NDQ0NDQiXQAAACQyMjIyMjIyMi0yMjIyLTQyMjItODIyMi0yMjIyMjIyMjIyMjIAAABcWyJhY2NvdW50LWlkIiwiOTk5OTk5OTktOTk5OS00OTk5LTg5OTktOTk5OTk5OTk5OTk5IiwiNDQ0NDQ0NDQtNDQ0NC00NDQ0LTg0NDQtNDQ0NDQ0NDQ0NDQ0Il0AAAACu8w=",
  ];
  const actual = [
    spaceRootBytes("fixture", root),
    spaceGrantBytes("fixture", root, grant),
    spaceDeliveryBytes("fixture", root, delivery),
  ];
  actual.forEach((bytes, index) =>
    expect(bytes).toEqual(new Uint8Array(Buffer.from(expected[index]!, "base64"))),
  );
});

it("rejects signatures transplanted between coordinates, recipients and envelope bytes", async () => {
  const signer = await createBrowserDeviceSigner();
  const device = await signer.createKeyPair();
  try {
    const signed = spaceRootBytes("fixture", root);
    const signature = await signer.sign(device.privateKey, signed);
    expect(await signer.verify(device.publicKey, signed, signature)).toBe(true);
    for (const changes of [
      { id: delivery.id },
      { scopeId: delivery.id },
      { identifier: delivery.id },
      { epochId: delivery.id },
      { accountId: "other-account" },
      { deviceId: delivery.id },
      { accountEpochId: delivery.id },
      { initialGrantId: delivery.id },
      { mechanism: "other" },
      { version: 2 },
      { verification: new Uint8Array([1]) },
      { authorEnvelope: new Uint8Array([1]) },
    ])
      expect(
        await signer.verify(
          device.publicKey,
          spaceRootBytes("fixture", { ...root, ...changes }),
          signature,
        ),
      ).toBe(false);
    expect(
      await signer.verify(device.publicKey, spaceRootBytes("other-app", root), signature),
    ).toBe(false);
    for (const [original, mutations] of [
      [
        spaceGrantBytes("fixture", root, grant),
        [
          spaceGrantBytes("fixture", root, { ...grant, operation: "remove" }),
          spaceGrantBytes("fixture", root, { ...grant, recipientKind: "group" }),
          spaceGrantBytes("fixture", root, { ...grant, recipientId: "other-account" }),
          spaceGrantBytes("fixture", root, { ...grant, recipientEpochId: delivery.id }),
        ],
      ],
      [
        spaceDeliveryBytes("fixture", root, delivery),
        [
          spaceDeliveryBytes("fixture", root, { ...delivery, recipientDeviceId: root.deviceId }),
          spaceDeliveryBytes("fixture", root, { ...delivery, recipientAccountId: "other-account" }),
          spaceDeliveryBytes("fixture", root, { ...delivery, senderDeviceId: delivery.id }),
          spaceDeliveryBytes("fixture", root, { ...delivery, envelope: new Uint8Array([1]) }),
        ],
      ],
    ] as const) {
      const signed = await signer.sign(device.privateKey, original);
      for (const mutation of mutations)
        expect(await signer.verify(device.publicKey, mutation, signed)).toBe(false);
    }
    expect(() =>
      spaceContext("fixture", { ...root, scopeId: "projects" }, "verification"),
    ).toThrow();
    expect(() => spaceGrantBytes("fixture", root, { ...grant, spaceId: delivery.id })).toThrow();
    expect(() =>
      spaceDeliveryBytes("fixture", root, { ...delivery, epochId: delivery.id }),
    ).toThrow();
  } finally {
    device.privateKey.fill(0);
  }
});
