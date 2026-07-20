import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createEncryptedValueStore } from "./oauth-session-store.js";
import { loadOrCreateJwtKeys, type StoredJwtKeys } from "./signing-keys.js";
import { withTestDatabase } from "./test-support/database.js";

describe("Jazz JWT signing keys", () => {
  beforeEach(() => vi.stubEnv("OAUTH_SESSION_ENCRYPTION_KEY", "00".repeat(32)));
  afterEach(() => vi.unstubAllEnvs());

  it("reuses the signing key after the BFF restarts", async () => {
    await withTestDatabase(async (database) => {
      const createStore = () => createEncryptedValueStore<StoredJwtKeys>(
        database,
        "jazz-signing-key:",
      );
      const firstKeys = await loadOrCreateJwtKeys(createStore());
      const restartedKeys = await loadOrCreateJwtKeys(createStore());

      expect(restartedKeys).toEqual(firstKeys);
    });
  });
});
