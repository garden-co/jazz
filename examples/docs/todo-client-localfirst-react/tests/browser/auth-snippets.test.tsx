import { describe, expect, it } from "vitest";
import { createJazzClient } from "jazz-tools/client";
import { getRecoveryPhrase, restoreRecoveryPhrase } from "../../src/auth-snippets.js";
import { memoryAccountStore } from "./memory-account-store.js";
import { SCOPE, SERVER_URL } from "./test-constants.js";

describe("device account recovery snippets", () => {
  it("restores the same device account after sign-out through the session", async () => {
    const session = await createJazzClient({
      scope: SCOPE,
      serverUrl: SERVER_URL,
      initial: "device",
      store: memoryAccountStore(),
    });
    try {
      const original = session.getSnapshot().user!;
      expect(original.kind).toBe("device");
      const phrase = getRecoveryPhrase(session);
      await session.signOut();
      expect(session.getSnapshot().user).toBeNull();
      await restoreRecoveryPhrase(session, phrase);
      const restored = session.getSnapshot().user!;
      expect(restored.accountId).toBe(original.accountId);
      expect(restored.identity).toEqual(original.identity);
      expect(session.getSnapshot().status).toBe("ready");
      expect(session.getSnapshot().client).toBeDefined();
      expect(getRecoveryPhrase(session)).toBe(phrase);
    } finally {
      await session.close();
    }
  });
});
