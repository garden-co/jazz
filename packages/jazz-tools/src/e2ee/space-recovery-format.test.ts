import { expect, it } from "vitest";
import { createNativeDeviceSigner } from "./native.js";
import { spaceDeliveryBytes } from "./space-format.js";
import { spaceRecoveryBytes, spaceRecoveryContext } from "./space-recovery-format.js";

it("pins space recovery framing and rejects transplanted coordinates", async () => {
  const root = {
    id: "11111111-1111-4111-8111-111111111111",
    epochId: "22222222-2222-4222-8222-222222222222",
    scopeId: "77777777-7777-4777-8777-777777777777",
    identifier: "88888888-8888-4888-8888-888888888888",
  };
  const record = {
    id: "55555555-5555-4555-8555-555555555555",
    spaceId: root.id,
    epochId: root.epochId,
    senderAccountId: "sender",
    senderDeviceId: "33333333-3333-4333-8333-333333333333",
    senderEpochId: "44444444-4444-4444-8444-444444444444",
    recipientAccountId: "recipient",
    recipientEpochId: "44444444-4444-4444-8444-444444444444",
    recoveryRootId: "66666666-6666-4666-8666-666666666666",
    envelope: Uint8Array.of(1, 2, 3),
  };
  // Independently emitted by SPEC/fixtures/e2ee-space-recovery.c.
  const expected =
    "000001cd4a453243010000000766697874757265000000126a617a7a2e653265652e73706163652e76310000002437373737373737372d373737372d343737372d383737372d3737373737373737373737370000002438383838383838382d383838382d343838382d383838382d3838383838383838383838380000000d5f5f653265655f7370616365730000002431313131313131312d313131312d343131312d383131312d313131313131313131313131000000935b227265636f766572792d64656c6976657279222c2235353535353535352d353535352d343535352d383535352d353535353535353535353535222c2273656e646572222c2233333333333333332d333333332d343333332d383333332d333333333333333333333333222c2234343434343434342d343434342d343434342d383434342d343434343434343434343434225d0000002432323232323232322d323232322d343232322d383232322d3232323232323232323232320000005b5b22726563697069656e74222c2236363636363636362d363636362d343636362d383636362d363636363636363636363636222c2234343434343434342d343434342d343434342d383434342d343434343434343434343434225d00000003010203";
  const bytes = spaceRecoveryBytes("fixture", root, record);
  expect(Buffer.from(bytes).toString("hex")).toBe(expected);
  const signer = await createNativeDeviceSigner();
  const keys = await signer.createKeyPair();
  const other = "99999999-9999-4999-8999-999999999999";
  try {
    const signature = await signer.sign(keys.privateKey, bytes);
    expect(await signer.verify(keys.publicKey, bytes, signature)).toBe(true);
    for (const field of [
      "id",
      "senderAccountId",
      "senderDeviceId",
      "senderEpochId",
      "recipientAccountId",
      "recipientEpochId",
      "recoveryRootId",
    ] as const) {
      expect(
        await signer.verify(
          keys.publicKey,
          spaceRecoveryBytes("fixture", root, { ...record, [field]: other }),
          signature,
        ),
      ).toBe(false);
    }
    for (const field of ["scopeId", "identifier"] as const)
      expect(
        await signer.verify(
          keys.publicKey,
          spaceRecoveryBytes("fixture", { ...root, [field]: other }, record),
          signature,
        ),
      ).toBe(false);
    for (const field of ["id", "epochId"] as const) {
      const changed = { ...root, [field]: other };
      const delivery = { ...record, spaceId: changed.id, epochId: changed.epochId };
      expect(
        await signer.verify(
          keys.publicKey,
          spaceRecoveryBytes("fixture", changed, delivery),
          signature,
        ),
      ).toBe(false);
      expect(() => spaceRecoveryContext("fixture", changed, record)).toThrow();
    }
    expect(
      await signer.verify(keys.publicKey, spaceRecoveryBytes("other", root, record), signature),
    ).toBe(false);
    expect(
      await signer.verify(
        keys.publicKey,
        spaceRecoveryBytes("fixture", root, { ...record, envelope: Uint8Array.of(1, 2, 4) }),
        signature,
      ),
    ).toBe(false);
    expect(
      await signer.verify(
        keys.publicKey,
        spaceDeliveryBytes("fixture", root, {
          ...record,
          recipientDeviceId: record.recoveryRootId,
        }),
        signature,
      ),
    ).toBe(false);
  } finally {
    keys.privateKey.fill(0);
  }
  for (const field of [
    "id",
    "senderDeviceId",
    "senderEpochId",
    "recipientEpochId",
    "recoveryRootId",
  ] as const)
    expect(() =>
      spaceRecoveryContext("fixture", root, { ...record, [field]: "invalid" }),
    ).toThrow();
  for (const field of ["senderAccountId", "recipientAccountId"] as const)
    expect(() => spaceRecoveryContext("fixture", root, { ...record, [field]: "" })).toThrow();
});
