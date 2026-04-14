import { describe, it, expect } from "vitest";
import type { DbConfig } from "./db.js";

describe("DbConfig auth validation", () => {
  it("rejects setting both auth.localFirstSecret and jwtToken", async () => {
    const { createDb } = await import("./db.js");
    const config: DbConfig = {
      appId: "test-app",
      auth: { localFirstSecret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" },
      jwtToken: "some-jwt",
    };
    await expect(createDb(config)).rejects.toThrow("mutually exclusive");
  });
});

describe("getLocalFirstIdentityProof", () => {
  it("returns a token for a local-first session", async () => {
    const { createDb } = await import("./db.js");
    const db = await createDb({
      appId: "test-app",
      auth: { localFirstSecret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" },
    });

    const token = await db.getLocalFirstIdentityProof({ audience: "test-audience" });
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

    const token = await db.getLocalFirstIdentityProof({ audience: "test-audience" });
    expect(token).toBeNull();
    await db.shutdown();
  });
});
