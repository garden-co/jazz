import { expect, it } from "vitest";
import { createNativeCrypto } from "./native.js";
import { protectRecoveryMaterial, openRecoveryMaterial } from "./recovery-protection.js";
import { recoveryProtectionContext } from "./recovery-protection.js";

it("pins the independently length-prefixed recovery protection context", () => {
  // Literal JE2C bytes constructed with independent u32be UTF-8 field framing.
  expect(
    recoveryProtectionContext({
      application: "fixture",
      accountId: "account-id",
      rootId: "11111111-1111-4111-8111-111111111111",
    }),
  ).toEqual(
    new Uint8Array(
      Buffer.from(
        "SkUyQwEAAAAHZml4dHVyZQAAACZqYXp6LmUyZWUubG9jYWwtcmVjb3ZlcnktcHJvdGVjdGlvbi52MQAAAAdhY2NvdW50AAAACmFjY291bnQtaWQAAAAaX19lMmVlX3JlY292ZXJ5X3Byb3RlY3RvcnMAAAAkMTExMTExMTEtMTExMS00MTExLTgxMTEtMTExMTExMTExMTExAAAACG1hdGVyaWFsAAAAJDExMTExMTExLTExMTEtNDExMS04MTExLTExMTExMTExMTExMQAAACQxMTExMTExMS0xMTExLTQxMTEtODExMS0xMTExMTExMTExMTE=",
        "base64",
      ),
    ),
  );
});

it("protects recovery material with account, application and root binding", async () => {
  const { cellCipher } = await createNativeCrypto();
  const secret = new Uint8Array(32).fill(7);
  const target = {
    application: "fixture",
    accountId: "account-id",
    rootId: "11111111-1111-4111-8111-111111111111",
  };
  const material = "client-only recovery material";
  const envelope = await protectRecoveryMaterial(cellCipher, secret, target, material);
  expect(new TextDecoder().decode(envelope)).not.toContain(material);
  expect(await openRecoveryMaterial(cellCipher, secret, target, envelope)).toBe(material);
  for (const altered of [
    { ...target, application: "other-app" },
    { ...target, accountId: "other-account" },
    { ...target, rootId: "22222222-2222-4222-8222-222222222222" },
  ])
    await expect(openRecoveryMaterial(cellCipher, secret, altered, envelope)).rejects.toThrow();
  await expect(
    openRecoveryMaterial(cellCipher, new Uint8Array(32).fill(8), target, envelope),
  ).rejects.toThrow();
  const corrupt = Uint8Array.from(envelope);
  corrupt[corrupt.length - 1] ^= 1;
  await expect(openRecoveryMaterial(cellCipher, secret, target, corrupt)).rejects.toThrow();
  expect(secret).toEqual(new Uint8Array(32).fill(7));
});
