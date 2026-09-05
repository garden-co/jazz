import { describe, it, expect } from "vitest";
import type { DbConfig } from "./db.js";
import type { Session } from "./context.js";
import { formatAuthSecret } from "./auth-secret-codec.js";

const SECRET = formatAuthSecret(new Uint8Array(32));

const cookieSession: Session = {
  user_id: "alice",
  claims: { role: "reader" },
  issuer: "https://issuer.example",
  authMode: "external",
};

describe("DbConfig auth validation", () => {
  it("rejects setting both secret and jwtToken", async () => {
    const { createDb } = await import("./default-create-db.js");
    // @ts-expect-error Exercise the runtime guard for untyped JavaScript callers.
    const config: DbConfig = {
      appId: "test-app",
      secret: SECRET,
      jwtToken: "some-jwt",
    };
    await expect(createDb(config)).rejects.toThrow("mutually exclusive");
  });

  it("rejects setting both jwtToken and cookieSession", async () => {
    const { createDb } = await import("./default-create-db.js");
    // @ts-expect-error Exercise the runtime guard for untyped JavaScript callers.
    const config: DbConfig = {
      appId: "test-app",
      jwtToken: "some-jwt",
      cookieSession,
    };
    await expect(createDb(config)).rejects.toThrow("mutually exclusive");
  });

  it("rejects setting both secret and cookieSession from untyped callers", async () => {
    const { createDb } = await import("./default-create-db.js");
    const config = {
      appId: "test-app",
      secret: SECRET,
      cookieSession,
    } as unknown as DbConfig;

    await expect(createDb(config)).rejects.toThrow("mutually exclusive");
  });

  it("accepts flat secret field", async () => {
    const { createDb } = await import("./default-create-db.js");
    const db = await createDb({
      appId: "test-app",
      secret: SECRET,
    });
    expect(db).toBeDefined();
    expect(db.getConfig()).toMatchObject({ jwtToken: expect.any(String) });
    expect(db.getConfig().secret).toBeUndefined();
    expect(db.getConfig()).not.toHaveProperty("trustedReservedSession");
    await db.shutdown();
  });
});

describe("getLocalFirstIdentityProof", () => {
  it("returns a token for a local-first session", async () => {
    const { createDb } = await import("./default-create-db.js");
    const db = await createDb({
      appId: "test-app",
      secret: SECRET,
    });

    const token = db.getLocalFirstIdentityProof({ audience: "test-audience" });
    expect(token).toBeTypeOf("string");
    expect(token!.split(".")).toHaveLength(3);
    await db.shutdown();
  });

  it("returns null for a non-local-first session", async () => {
    const { createDb } = await import("./default-create-db.js");
    const db = await createDb({
      appId: "test-app",
      jwtToken: "dummy-jwt",
    });

    const token = db.getLocalFirstIdentityProof({ audience: "test-audience" });
    expect(token).toBeNull();
    await db.shutdown();
  });

  it("rejects an unversioned secret before runtime or author minting", async () => {
    const { createDb } = await import("./default-create-db.js");
    await expect(
      createDb({ appId: "test-app", secret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" }),
    ).rejects.toThrow(/jazz-auth-v1/);
  });

  it("rejects backend admission credentials on the public client factory", async () => {
    const { createDb } = await import("./default-create-db.js");
    await expect(
      createDb({ appId: "test-app", backendSecret: "server-only" } as unknown as DbConfig),
    ).rejects.toThrow(/createJazzContext/);
  });
});
