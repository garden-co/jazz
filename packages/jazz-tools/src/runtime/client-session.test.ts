import { describe, expect, it } from "vitest";
import { resolveClientSessionSync, resolveClientSessionStateSync } from "./client-session.js";

function toBase64Url(value: string): string {
  return Buffer.from(value, "utf8")
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function makeJwt(payload: Record<string, unknown>): string {
  const header = { alg: "none", typ: "JWT" };
  return `${toBase64Url(JSON.stringify(header))}.${toBase64Url(JSON.stringify(payload))}.`;
}

describe("client session resolution", () => {
  it("prefers jazz_principal_id from JWT when present", () => {
    const jwt = makeJwt({
      sub: "user-subject",
      jazz_principal_id: "principal-123",
      iss: "https://issuer.example",
      claims: { role: "editor" },
    });

    const session = resolveClientSessionSync({
      appId: "app-jwt-principal",
      jwtToken: jwt,
    });

    expect(session).toEqual({
      user_id: "principal-123",
      claims: {
        role: "editor",
        auth_mode: "external",
        subject: "user-subject",
        issuer: "https://issuer.example",
      },
    });
  });

  it("falls back to JWT sub when principal claim is absent", () => {
    const jwt = makeJwt({
      sub: "user-subject",
      claims: { team: "eng" },
    });

    const session = resolveClientSessionSync({
      appId: "app-jwt-sub",
      jwtToken: jwt,
    });

    expect(session).toEqual({
      user_id: "user-subject",
      claims: {
        team: "eng",
        auth_mode: "external",
        subject: "user-subject",
      },
    });
  });

  it("returns null session when jwt cannot be parsed", () => {
    const state = resolveClientSessionStateSync({
      appId: "fallback-app",
      jwtToken: "not-a-jwt",
    });

    expect(state.transport).toBeNull();
    expect(state.session).toBeNull();
  });

  it("returns null when no auth is configured", () => {
    expect(resolveClientSessionSync({ appId: "no-auth" })).toBeNull();
    expect(resolveClientSessionStateSync({ appId: "no-auth" })).toEqual({
      transport: null,
      session: null,
    });
  });
});
