import { describe, expect, it } from "vitest";
import { resolveDefaultPersistentDbName, type DbConfig } from "./db.js";

function toBase64Url(value: unknown): string {
  return Buffer.from(JSON.stringify(value), "utf8")
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function makeJwt(payload: Record<string, unknown>): string {
  return `${toBase64Url({ alg: "HS256", typ: "JWT" })}.${toBase64Url(payload)}.signature`;
}

describe("resolveDefaultPersistentDbName", () => {
  it("keeps an explicit driver dbName unchanged", () => {
    const config: DbConfig = {
      appId: "chat-app",
      driver: { type: "persistent", dbName: "custom-db" },
      jwtToken: makeJwt({ sub: "alice" }),
    };

    expect(resolveDefaultPersistentDbName(config)).toBe("custom-db");
  });

  it("scopes the default namespace by user_id when a session is present", () => {
    const config: DbConfig = {
      appId: "chat-app",
      driver: { type: "persistent" },
      jwtToken: makeJwt({ sub: "alice@example.com" }),
    };

    expect(resolveDefaultPersistentDbName(config)).toBe("chat-app::alice%40example.com");
  });

  it("prefers jazz_principal_id when present in the JWT", () => {
    const config: DbConfig = {
      appId: "chat-app",
      driver: { type: "persistent" },
      jwtToken: makeJwt({
        sub: "subject-123",
        jazz_principal_id: "principal/456",
      }),
    };

    expect(resolveDefaultPersistentDbName(config)).toBe("chat-app::principal%2F456");
  });

  it("falls back to appId when no session can be resolved", () => {
    const config: DbConfig = {
      appId: "chat-app",
      driver: { type: "persistent" },
    };

    expect(resolveDefaultPersistentDbName(config)).toBe("chat-app");
  });
});
