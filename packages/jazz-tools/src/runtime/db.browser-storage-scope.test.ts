import { describe, expect, it } from "vitest";
import { resolveDefaultPersistentDbName, type DbConfig } from "./db.js";
import { ANONYMOUS_JWT_ISSUER, LOCAL_FIRST_JWT_ISSUER } from "./client-session.js";

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

  it("url-encodes the sub when scoping the namespace", () => {
    const config: DbConfig = {
      appId: "chat-app",
      driver: { type: "persistent" },
      jwtToken: makeJwt({ sub: "principal/456" }),
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

  it("does not scope by user_id for anonymous sessions", () => {
    const config: DbConfig = {
      appId: "chat-app",
      driver: { type: "persistent" },
      jwtToken: makeJwt({ sub: "ephemeral-pubkey", iss: ANONYMOUS_JWT_ISSUER }),
    };

    expect(resolveDefaultPersistentDbName(config)).toBe("chat-app");
  });

  it("scopes by user_id for local-first sessions", () => {
    const config: DbConfig = {
      appId: "chat-app",
      driver: { type: "persistent" },
      jwtToken: makeJwt({ sub: "stable-pubkey", iss: LOCAL_FIRST_JWT_ISSUER }),
    };

    expect(resolveDefaultPersistentDbName(config)).toBe("chat-app::stable-pubkey");
  });
});
