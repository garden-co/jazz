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
});

describe("getSelfSignedToken", () => {
  it("returns a token for a self-signed session", async () => {
    const { createDb } = await import("./db.js");
    const db = await createDb({
      appId: "test-app",
      auth: { seed: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" },
    });

    const token = await db.getSelfSignedToken({ audience: "test-audience" });
    expect(token).toBeTypeOf("string");
    expect(token!.split(".")).toHaveLength(3);
    await db.shutdown();
  });

  it("returns null for a non-self-signed session", async () => {
    const { createDb } = await import("./db.js");
    const db = await createDb({
      appId: "test-app",
      jwtToken: "dummy-jwt",
    });

    const token = await db.getSelfSignedToken({ audience: "test-audience" });
    expect(token).toBeNull();
    await db.shutdown();
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
