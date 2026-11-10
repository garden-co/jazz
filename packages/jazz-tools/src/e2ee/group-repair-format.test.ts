import { expect, it } from "vitest";
import { groupRepairBytes } from "./group-format.js";
import { createNativeDeviceSigner } from "./native.js";

it("binds a group repair request to the requesting device and rejected delivery", async () => {
  const id = "11111111-1111-4111-8111-111111111111";
  const record = {
    id,
    groupId: id,
    epochId: id,
    deliveryId: id,
    accountId: "account",
    deviceId: id,
    accountEpochId: id,
  };
  const bytes = groupRepairBytes("fixture", record);
  // Literal JE2C fixture framed independently of the production encoder.
  expect(Buffer.from(bytes).toString("hex")).toBe(
    "4a453243010000000766697874757265000000196a617a7a2e653265652e67726f75702d7265706169722e76310000000567726f75700000002431313131313131312d313131312d343131312d383131312d313131313131313131313131000000145f5f653265655f67726f75705f726570616972730000002431313131313131312d313131312d343131312d383131312d313131313131313131313131000000595b2231313131313131312d313131312d343131312d383131312d313131313131313131313131222c226163636f756e74222c2231313131313131312d313131312d343131312d383131312d313131313131313131313131225d0000002431313131313131312d313131312d343131312d383131312d3131313131313131313131310000002431313131313131312d313131312d343131312d383131312d313131313131313131313131",
  );
  const signer = await createNativeDeviceSigner();
  const key = await signer.createKeyPair();
  try {
    const signature = await signer.sign(key.privateKey, bytes);
    expect(await signer.verify(key.publicKey, bytes, signature)).toBe(true);
    for (const field of Object.keys(record)) {
      const changed = { ...record, [field]: "22222222-2222-4222-8222-222222222222" };
      expect(
        await signer.verify(key.publicKey, groupRepairBytes("fixture", changed), signature),
      ).toBe(false);
    }
    expect(await signer.verify(key.publicKey, groupRepairBytes("other", record), signature)).toBe(
      false,
    );
    expect(() => groupRepairBytes("fixture", { ...record, deliveryId: "invalid" })).toThrow();
  } finally {
    key.privateKey.fill(0);
  }
});
