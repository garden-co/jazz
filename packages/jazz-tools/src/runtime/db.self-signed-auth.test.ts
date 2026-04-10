import { describe, it, expect } from "vitest";
import type { DbConfig } from "./db.js";

describe("DbConfig auth validation", () => {
  it("rejects setting both auth.seed and jwtToken", async () => {
    const { createDb } = await import("./db.js");
    const config: DbConfig = {
      appId: "test-app",
      auth: { seed: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" },
      jwtToken: "some-jwt",
    };
    await expect(createDb(config)).rejects.toThrow("mutually exclusive");
  });

  it("rejects setting both auth.seedStore and jwtToken", async () => {
    const { createDb } = await import("./db.js");
    const fakeSeedStore = {
      loadSeed: async () => null,
      saveSeed: async () => {},
      clearSeed: async () => {},
      getOrCreateSeed: async () => "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    };
    const config: DbConfig = {
      appId: "test-app",
      auth: { seedStore: fakeSeedStore },
      jwtToken: "some-jwt",
    };
    await expect(createDb(config)).rejects.toThrow("mutually exclusive");
  });
});

describe("resolveLocalAuthDefaults with auth", () => {
  it("skips local auth defaults when auth is set", async () => {
    const { resolveLocalAuthDefaults } = await import("./local-auth.js");
    const config = {
      appId: "test-app",
      auth: { seed: "test-seed" },
    };
    const result = resolveLocalAuthDefaults(config);
    expect(result.localAuthMode).toBeUndefined();
    expect(result.localAuthToken).toBeUndefined();
  });
});
