import { expect, it } from "vitest";
import { groupRecoveryContext, groupRecoveryBytes } from "./group-recovery-format.js";
import { createNativeDeviceSigner } from "./native.js";

it("pins group recovery delivery framing and every signed coordinate", async () => {
  const id = "11111111-1111-4111-8111-111111111111";
  const record = {
    id,
    groupId: id,
    epochId: id,
    senderAccountId: "sender",
    senderDeviceId: id,
    recipientAccountId: "recipient",
    recoveryRootId: id,
    envelope: Uint8Array.of(1, 2, 3),
  };
  // Independently framed with Python struct.pack('>I', length), not the encoder.
  const contextHex =
    "4a4532430100000007666978747572650000001b6a617a7a2e653265652e67726f75702d7265636f766572792e76310000000567726f75700000002431313131313131312d313131312d343131312d383131312d313131313131313131313131000000205f5f653265655f67726f75705f7265636f766572795f64656c697665726965730000002431313131313131312d313131312d343131312d383131312d313131313131313131313131000000315b2273656e646572222c2231313131313131312d313131312d343131312d383131312d313131313131313131313131225d0000002431313131313131312d313131312d343131312d383131312d313131313131313131313131000000345b22726563697069656e74222c2231313131313131312d313131312d343131312d383131312d313131313131313131313131225d";
  expect(Buffer.from(groupRecoveryContext("fixture", record)).toString("hex")).toBe(contextHex);
  const bytes = groupRecoveryBytes("fixture", record);
  expect(Buffer.from(bytes).toString("hex")).toBe("00000141" + contextHex + "00000003010203");
  const signer = await createNativeDeviceSigner();
  const key = await signer.createKeyPair();
  try {
    const signature = await signer.sign(key.privateKey, bytes);
    expect(await signer.verify(key.publicKey, bytes, signature)).toBe(true);
    for (const field of [
      "id",
      "groupId",
      "epochId",
      "senderAccountId",
      "senderDeviceId",
      "recipientAccountId",
      "recoveryRootId",
    ] as const) {
      const changed = { ...record, [field]: "22222222-2222-4222-8222-222222222222" };
      expect(
        await signer.verify(key.publicKey, groupRecoveryBytes("fixture", changed), signature),
      ).toBe(false);
    }
    expect(await signer.verify(key.publicKey, groupRecoveryBytes("other", record), signature)).toBe(
      false,
    );
    expect(
      await signer.verify(
        key.publicKey,
        groupRecoveryBytes("fixture", { ...record, envelope: Uint8Array.of(1, 2, 4) }),
        signature,
      ),
    ).toBe(false);
  } finally {
    key.privateKey.fill(0);
  }
  for (const field of ["id", "groupId", "epochId", "senderDeviceId", "recoveryRootId"] as const)
    expect(() => groupRecoveryContext("fixture", { ...record, [field]: "invalid" })).toThrow();
  for (const field of ["senderAccountId", "recipientAccountId"] as const)
    expect(() => groupRecoveryContext("fixture", { ...record, [field]: "" })).toThrow();
});
