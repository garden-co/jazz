import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { loadOrCreateJazzSigningKeys } from "./signing-keys.js";
import { withTestDatabase } from "./test-support/database.js";

describe("Jazz JWT signing keys", () => {
  beforeEach(() => vi.stubEnv("OAUTH_SESSION_ENCRYPTION_KEY", "00".repeat(32)));
  afterEach(() => vi.unstubAllEnvs());

  it("reuses persisted signing keys", async () => {
    await withTestDatabase(async (database) => {
      const firstKeys = await loadOrCreateJazzSigningKeys(database);
      const persistedKeys = await loadOrCreateJazzSigningKeys(database);

      expect(persistedKeys).toEqual(firstKeys);
    });
  });
});
