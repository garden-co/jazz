import { it, expect } from "vitest";
import { prepareAccountManager } from "../accounts/persistence.js";
import { accountToken } from "../accounts/enrollment.js";
import { generateAuthSecret } from "../runtime/auth-secret-store.js";
it("retaining a different root cannot release failed original credentials", async () => {
  let durable: string | null = null;
  const failure = new Error("original root retention failed");
  const registry = "https://core.example/apps/test/accounts";
  const accounts = await prepareAccountManager({
    appId: "test",
    registry,
    mintToken: () =>
      `e30.${btoa(JSON.stringify({ iss: "urn:jazz:local-first", sub: "00000000-0000-4000-8000-000000000001" }))}.sig`,
    store: {
      read: async () => durable,
      update: async () => {
        throw failure;
      },
    },
  });
  const original = accounts.createLocalFirst();
  await expect(accountToken(original, registry)).rejects.toBe(failure);
  durable = JSON.stringify({
    format: "jazz-account-selection-v1",
    roots: [generateAuthSecret()],
    selected: 0,
  });
  await expect(accountToken(original, registry)).rejects.toBe(failure);
});
