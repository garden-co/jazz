import { expect, it } from "vitest";
import { createNativeCrypto } from "./native.js";
import { encodeRecoveryMaterial, decodeRecoveryMaterial } from "./recovery-format.js";

it("validates recovery material against the account scope and both cryptographic keypairs", async () => {
  const crypto = await createNativeCrypto();
  const pair = await crypto.keyEnvelope.createKeyPair();
  const signing = await crypto.deviceSigner.createKeyPair();
  try {
    const root = {
      id: "11111111-1111-4111-8111-111111111111",
      accountId: "account-id",
      signerId: "22222222-2222-4222-8222-222222222222",
      epochId: "33333333-3333-4333-8333-333333333333",
      publicKey: pair.publicKey,
      mechanism: crypto.keyEnvelope.mechanism.id,
      version: crypto.keyEnvelope.mechanism.version,
      signingPublicKey: signing.publicKey,
      signingMechanism: crypto.deviceSigner.mechanism.id,
      signingVersion: crypto.deviceSigner.mechanism.version,
    };
    const material = encodeRecoveryMaterial(
      "account-scope",
      root,
      pair.privateKey,
      signing.privateKey,
    );
    const decode = (value: string, scope = "account-scope") =>
      decodeRecoveryMaterial(value, scope, crypto.keyEnvelope, crypto.deviceSigner);
    const restored = await decode(material);
    try {
      expect(restored.rootId).toBe(root.id);
      expect(restored.recipient.publicKey).toEqual(pair.publicKey);
      expect(restored.recipient.privateKey).toEqual(pair.privateKey);
      expect(restored.signing.publicKey).toEqual(signing.publicKey);
      expect(restored.signing.privateKey).toEqual(signing.privateKey);
    } finally {
      restored.recipient.privateKey.fill(0);
      restored.signing.privateKey.fill(0);
    }
    await expect(decode(material, "other-account")).rejects.toMatchObject({
      code: "recovery-material-unusable",
    });
    for (const patch of [
      { format: "jazz-e2ee-recovery-v2" },
      { rootId: "not-an-id" },
      { privateKey: [] },
      { publicKey: [256] },
      { signingPrivateKey: [0.5] },
      { privateKey: Array.from({ length: 65537 }, () => 1) },
      { mechanism: "other" },
      { signingVersion: 2 },
      { unexpected: "field" },
      { publicKey: Array.from({ length: 32 }, () => 0) },
      { signingPublicKey: Array.from({ length: 32 }, () => 0) },
    ]) {
      await expect(decode(JSON.stringify({ ...JSON.parse(material), ...patch }))).rejects.toThrow();
    }
    await expect(decode("null")).rejects.toThrow();
    await expect(decode("{")).rejects.toThrow();
    // A bad import must not consume or mutate a valid caller-owned material string.
    const retried = await decode(material);
    retried.recipient.privateKey.fill(0);
    retried.signing.privateKey.fill(0);
  } finally {
    pair.privateKey.fill(0);
    signing.privateKey.fill(0);
  }
});
