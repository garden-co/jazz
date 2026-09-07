import { describe, expect, it } from "vitest";
import { createJazzSession } from "jazz-tools/client";
import { getRecoveryPhrase, restoreRecoveryPhrase } from "../../src/auth-snippets.js";
import { APP_ID, TEST_PORT } from "./test-constants.js";

describe("account recovery snippets", () => {
  it("restores the same account and identity after logout through the session", async () => {
    const session = await createJazzSession({
      appId: APP_ID,
      serverUrl: `http://127.0.0.1:${TEST_PORT}`,
      initial: "local-first",
    });
    try {
      const original = session.getSnapshot().account!;
      const phrase = getRecoveryPhrase(original);
      await session.logout();
      expect(session.getSnapshot().account).toBeUndefined();
      await restoreRecoveryPhrase(session, phrase);
      const restored = session.getSnapshot().account!;
      expect(restored.id).toBe(original.id);
      expect(restored.identity).toEqual(original.identity);
      expect(session.getSnapshot().status).toBe("ready");
      expect(session.getSnapshot().client).toBeDefined();
      expect(getRecoveryPhrase(restored)).toBe(phrase);
    } finally {
      await session.close();
    }
  });
});
