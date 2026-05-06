import { describe, it, expect } from "vitest";
import type { DbConfig } from "./db.js";

describe("DbConfig auth validation", () => {
  it("rejects setting both secret and jwtToken", async () => {
    const { createDb } = await import("./db.js");
    const config: DbConfig = {
      appId: "test-app",
      secret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
      jwtToken: "some-jwt",
    };
    await expect(createDb(config)).rejects.toThrow("mutually exclusive");
  });

  it("rejects setting both jwtToken and cookieSession", async () => {
    const { createDb } = await import("./db.js");
    const config: DbConfig = {
      appId: "test-app",
      jwtToken: "some-jwt",
      cookieSession: {
        user_id: "alice",
        claims: { role: "reader" },
        authMode: "external",
      },
    };
    await expect(createDb(config)).rejects.toThrow("mutually exclusive");
  });

  it("accepts flat secret field", async () => {
    const { createDb } = await import("./db.js");
    const db = await createDb({
      appId: "test-app",
      secret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    });
    expect(db).toBeDefined();
    await db.shutdown();
  });
});

describe("getLocalFirstIdentityProof", () => {
  it("returns a token for a local-first session", async () => {
    const { createDb } = await import("./db.js");
    const db = await createDb({
      appId: "test-app",
      secret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    });

    const token = db.getLocalFirstIdentityProof({ audience: "test-audience" });
    expect(token).toBeTypeOf("string");
    expect(token!.split(".")).toHaveLength(3);
    await db.shutdown();
  });

  it("returns null for a non-local-first session", async () => {
    const { createDb } = await import("./db.js");
    const db = await createDb({
      appId: "test-app",
      jwtToken: "dummy-jwt",
    });

    const token = db.getLocalFirstIdentityProof({ audience: "test-audience" });
    expect(token).toBeNull();
    await db.shutdown();
  });
});
