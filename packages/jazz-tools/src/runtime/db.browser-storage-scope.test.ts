import { describe, expect, it } from "vitest";
import { resolveDefaultPersistentDbName, type DbConfig } from "./db.js";
import { ANONYMOUS_JWT_ISSUER } from "./client-session.js";
import {
  internalSessionFromVerifiedReservedJwtPayload,
  LOCAL_FIRST_JWT_ISSUER,
} from "./client-session.js";
import {
  assertBrowserStorageOwnerUnchanged,
  createBrowserAuthSessionKey,
  createBrowserStorageOwner,
} from "./browser-worker-config.js";
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
  it("rejects a live persistent principal switch before forwarding it to storage", () => {
    const alice: DbConfig = {
      appId: "chat-app",
      jwtToken: makeJwt({ iss: "https://issuer.example", sub: "alice" }),
    };
    const refreshedAlice: DbConfig = {
      ...alice,
      jwtToken: makeJwt({ iss: "https://issuer.example", sub: "alice", exp: 2 }),
    };
    const bob: DbConfig = {
      ...alice,
      jwtToken: makeJwt({ iss: "https://issuer.example", sub: "bob" }),
    };

    expect(() => assertBrowserStorageOwnerUnchanged(alice, refreshedAlice)).not.toThrow();
    // Planted positive: removing the guard would let Bob inherit Alice's
    // durable root before an upstream auth response could correct it.
    expect(() => assertBrowserStorageOwnerUnchanged(alice, bob)).toThrow(
      "Cannot change the authenticated user of a live persistent browser Db",
    );
  });

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

  it("treats an explicit driver dbName as a scoped logical base", () => {
    const config: DbConfig = {
      appId: "chat-app",
      driver: { type: "persistent", dbName: "custom-db" },
      jwtToken: makeJwt({ sub: "alice" }),
    };

    const physicalName = resolveDefaultPersistentDbName(config);
    expect(physicalName).toMatch(/^custom-db::jazz-browser-v1::/);
    expect(physicalName).toContain("%22appId%22%3A%22chat-app%22");
    expect(physicalName).toContain("%22env%22%3A%22dev%22");
    expect(physicalName).not.toContain(config.jwtToken!);
  });

  it("scopes the default namespace by user_id when a session is present", () => {
    const config: DbConfig = {
      appId: "chat-app",
      driver: { type: "persistent" },
      jwtToken: makeJwt({ iss: "https://issuer.example", sub: "alice@example.com" }),
    };

    expect(resolveDefaultPersistentDbName(config)).toMatch(/^chat-app::jazz-browser-v1::/);
  });

  it("url-encodes the sub when scoping the namespace", () => {
    const config: DbConfig = {
      appId: "chat-app",
      driver: { type: "persistent" },
      jwtToken: makeJwt({ iss: "https://issuer.example", sub: "principal/456" }),
    };

    expect(resolveDefaultPersistentDbName(config)).toContain(
      "%5B%5C%22https%3A%2F%2Fissuer.example%5C%22%2C%5C%22principal%2F456%5C%22%5D",
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

    expect(resolveDefaultPersistentDbName(config)).toContain("alice%40example.com");
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

    expect(resolveDefaultPersistentDbName(config)).toMatch(/^chat-app::jazz-browser-v1::/);
  });

  it("falls back to appId when no session can be resolved", () => {
    const config: DbConfig = {
      appId: "chat-app",
      driver: { type: "persistent" },
    };

    expect(resolveDefaultPersistentDbName(config)).toMatch(/^chat-app::jazz-browser-v1::/);
  });

  it("does not scope by user_id for anonymous sessions", () => {
    const config: DbConfig = {
      appId: "chat-app",
      driver: { type: "persistent" },
      jwtToken: makeJwt({ sub: "ephemeral-pubkey", iss: ANONYMOUS_JWT_ISSUER }),
    };

    expect(resolveDefaultPersistentDbName(config)).toMatch(/^chat-app::jazz-browser-v1::/);
  });

  it("scopes by user_id for external sessions", () => {
    const config: DbConfig = {
      appId: "chat-app",
      driver: { type: "persistent" },
      jwtToken: makeJwt({ sub: "stable-pubkey", iss: "https://issuer.example" }),
    };

    expect(resolveDefaultPersistentDbName(config)).toContain("stable-pubkey");
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

  it("separates app, environment, anonymous, external, and local-first scopes without credentials", () => {
    const base = "shared-device-cache";
    const external = (appId: string, env: string, subject: string): DbConfig => ({
      appId,
      env,
      driver: { type: "persistent", dbName: base },
      jwtToken: makeJwt({ iss: "https://issuer.example/\u00e5", sub: subject }),
    });
    const alice = external("chat", "production", "alice/\ud83d\ude80");
    const bob = external("chat", "production", "bob/\ud83d\ude80");
    const localFirst: DbConfig = {
      appId: "chat",
      env: "production",
      secret: "local-only-secret",
      driver: { type: "persistent", dbName: base },
    };
    const anonymous: DbConfig = {
      appId: "chat",
      env: "production",
      driver: { type: "persistent", dbName: base },
    };

    const aliceName = resolveDefaultPersistentDbName(alice);
    expect(aliceName).toMatch(new RegExp(`^${base}::jazz-browser-v1::`));
    expect(aliceName).not.toBe(resolveDefaultPersistentDbName(bob));
    expect(aliceName).not.toBe(
      resolveDefaultPersistentDbName(external("other-app", "production", "alice/\ud83d\ude80")),
    );
    expect(aliceName).not.toBe(
      resolveDefaultPersistentDbName(external("chat", "staging", "alice/\ud83d\ude80")),
    );
    expect(aliceName).not.toBe(resolveDefaultPersistentDbName(localFirst));
    expect(aliceName).not.toBe(resolveDefaultPersistentDbName(anonymous));
    expect(resolveDefaultPersistentDbName(localFirst)).not.toContain("local-only-secret");
    expect(aliceName).not.toContain(alice.jwtToken!);
  });
});
