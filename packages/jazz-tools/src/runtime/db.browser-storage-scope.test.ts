import { describe, expect, it } from "vitest";
import { resolveDefaultPersistentDbName, type DbConfig } from "./db.js";
import { ANONYMOUS_JWT_ISSUER } from "./client-session.js";
import {
  internalSessionFromVerifiedReservedJwtPayload,
  LOCAL_FIRST_JWT_ISSUER,
} from "./client-session.js";
import { createBrowserAuthSessionKey, createBrowserStorageOwner } from "./browser-worker-config.js";
import { setTrustedReservedSession } from "./db-internal-session.js";

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
  it("uses the canonical public issuer/subject identity as the non-secret browser owner scope", () => {
    const config: DbConfig = {
      appId: "chat-app",
      env: "production",
      jwtToken: makeJwt({ iss: "https://issuer.example", sub: "alice@example.com" }),
    };

    // The durable marker and worker routing identity must preserve the exact
    // public session identity. A short hash could collide and silently let
    // two principals share an explicitly named physical root.
    expect(createBrowserAuthSessionKey(config)).toBe(
      '{"version":1,"appId":"chat-app","env":"production","auth":{"kind":"principal","authMode":"external","user":"[\\"https://issuer.example\\",\\"alice@example.com\\"]"}}',
    );
    expect(createBrowserStorageOwner(config)).toBe(
      '{"version":1,"appId":"chat-app","env":"production","auth":{"kind":"principal","authMode":"external","user":"[\\"https://issuer.example\\",\\"alice@example.com\\"]"}}',
    );
  });

  it("uses intentional anonymous and system auth-scope tags without credentials", () => {
    expect(createBrowserStorageOwner({ appId: "chat-app" })).toBe(
      '{"version":1,"appId":"chat-app","env":"dev","auth":{"kind":"anonymous"}}',
    );
    expect(createBrowserStorageOwner({ appId: "chat-app", adminSecret: "private" })).toBe(
      '{"version":1,"appId":"chat-app","env":"dev","auth":{"kind":"system"}}',
    );
  });

  it("keeps verified local-first principals distinct in browser worker auth metadata", () => {
    const configFor = (subject: string): DbConfig => {
      const config: DbConfig = {
        appId: "chat-app",
        jwtToken: makeJwt({ iss: LOCAL_FIRST_JWT_ISSUER, sub: subject }),
      };
      setTrustedReservedSession(
        config,
        internalSessionFromVerifiedReservedJwtPayload(
          { iss: LOCAL_FIRST_JWT_ISSUER, sub: subject },
          "local-first",
        ),
      );
      return config;
    };

    expect(createBrowserAuthSessionKey(configFor("alice"))).not.toBe(
      createBrowserAuthSessionKey(configFor("bob")),
    );
    expect(createBrowserStorageOwner(configFor("alice"))).not.toBe(
      createBrowserStorageOwner(configFor("bob")),
    );
  });

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
      jwtToken: makeJwt({ iss: "https://issuer.example", sub: "alice@example.com" }),
    };

    expect(resolveDefaultPersistentDbName(config)).toBe(
      "chat-app::%5B%22https%3A%2F%2Fissuer.example%22%2C%22alice%40example.com%22%5D",
    );
  });

  it("url-encodes the sub when scoping the namespace", () => {
    const config: DbConfig = {
      appId: "chat-app",
      driver: { type: "persistent" },
      jwtToken: makeJwt({ iss: "https://issuer.example", sub: "principal/456" }),
    };

    expect(resolveDefaultPersistentDbName(config)).toBe(
      "chat-app::%5B%22https%3A%2F%2Fissuer.example%22%2C%22principal%2F456%22%5D",
    );
  });

  it("scopes the default namespace by user_id for cookie sessions", () => {
    const config: DbConfig = {
      appId: "chat-app",
      driver: { type: "persistent" },
      cookieSession: {
        user_id: "alice@example.com",
        claims: {},
        issuer: "https://issuer.example",
        authMode: "external",
      },
    };

    expect(resolveDefaultPersistentDbName(config)).toBe(
      "chat-app::%5B%22https%3A%2F%2Fissuer.example%22%2C%22alice%40example.com%22%5D",
    );
  });

  it("does not scope by user_id for anonymous cookie sessions", () => {
    const config: DbConfig = {
      appId: "chat-app",
      driver: { type: "persistent" },
      cookieSession: {
        user_id: "ephemeral-visitor",
        claims: {},
        issuer: "urn:jazz:anonymous",
        authMode: "anonymous",
      },
    };

    expect(resolveDefaultPersistentDbName(config)).toBe("chat-app");
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

  it("scopes by user_id for external sessions", () => {
    const config: DbConfig = {
      appId: "chat-app",
      driver: { type: "persistent" },
      jwtToken: makeJwt({ sub: "stable-pubkey", iss: "https://issuer.example" }),
    };

    expect(resolveDefaultPersistentDbName(config)).toBe(
      "chat-app::%5B%22https%3A%2F%2Fissuer.example%22%2C%22stable-pubkey%22%5D",
    );
  });

  it("separates the same subject issued by different authorities", () => {
    const from = (issuer: string): DbConfig => ({
      appId: "chat-app",
      driver: { type: "persistent" },
      jwtToken: makeJwt({ iss: issuer, sub: "alice" }),
    });

    expect(resolveDefaultPersistentDbName(from("https://issuer-a.example"))).not.toBe(
      resolveDefaultPersistentDbName(from("https://issuer-b.example")),
    );
  });
});
