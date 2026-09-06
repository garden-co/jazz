import { describe, expect, it } from "vitest";
import { createAccountManager } from "jazz-tools";
import { getRecoveryPhrase, restoreRecoveryPhrase } from "../../src/auth-snippets.js";
import { APP_ID, TEST_PORT } from "./test-constants.js";

// Recovery is an account operation; no JazzProvider or live context is needed.
describe("account recovery snippets", () => {
  it("restores the same account and identity after logout without a context", async () => {
    const accounts = await createAccountManager({
      appId: APP_ID,
      serverUrl: `http://127.0.0.1:${TEST_PORT}`,
    });
    const original = accounts.createLocalFirst();
    const phrase = getRecoveryPhrase(original);
    accounts.logout();
    expect(accounts.getLoggedIn()).toBeUndefined();
    const restored = restoreRecoveryPhrase(accounts, phrase);
    expect(restored.id).toBe(original.id);
    expect(restored.identity).toEqual(original.identity);
    expect(accounts.getLoggedIn()).toBe(restored);
    expect(getRecoveryPhrase(restored)).toBe(phrase);
  });
});
