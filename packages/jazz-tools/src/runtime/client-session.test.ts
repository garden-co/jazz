import { describe, expect, it } from "vitest";
import type { Session } from "./context.js";
import {
  resolveClientSessionSync,
  resolveClientSessionStateSync,
  resolveJwtSession,
  LOCAL_FIRST_JWT_ISSUER,
  ANONYMOUS_JWT_ISSUER,
} from "./client-session.js";

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
  it("uses a mirrored cookie session when provided", () => {
    const session: Session = {
      user_id: "cookie-user",
      claims: {
        role: "writer",
        auth_mode: "external",
        subject: "subject-123",
        issuer: "https://issuer.example",
      },
      authMode: "external",
    };

    expect(
      resolveClientSessionStateSync({
        appId: "cookie-app",
        cookieSession: session,
      }),
    ).toEqual({
      transport: "cookie",
      session,
    });
  });

  it("uses JWT sub as user_id", () => {
    const jwt = makeJwt({
      sub: "user-subject",
      iss: "https://issuer.example",
      claims: { role: "editor" },
    });

    const session = resolveClientSessionSync({
      appId: "app-jwt-sub",
      jwtToken: jwt,
    });

    expect(session).toEqual({
      user_id: "user-subject",
      claims: {
        role: "editor",
        subject: "user-subject",
        issuer: "https://issuer.example",
      },
      authMode: "external",
    });
  });

  it("accepts a JWT with only a sub claim", () => {
    const jwt = makeJwt({
      sub: "user-subject",
      claims: { team: "eng" },
    });

    const session = resolveClientSessionSync({
      appId: "app-jwt-sub-only",
      jwtToken: jwt,
    });

    expect(session).toEqual({
      user_id: "user-subject",
      claims: {
        team: "eng",
        subject: "user-subject",
      },
      authMode: "external",
    });
  });

  it("returns null when no auth is configured", () => {
    expect(resolveClientSessionSync({ appId: "no-auth" })).toBeNull();
    expect(resolveClientSessionStateSync({ appId: "no-auth" })).toEqual({
      transport: null,
      session: null,
    });
  });
});

describe("resolveJwtSession — authMode derivation", () => {
  function jwt(payload: Record<string, unknown>): string {
    const header = Buffer.from(JSON.stringify({ alg: "EdDSA", typ: "JWT" })).toString("base64url");
    const body = Buffer.from(JSON.stringify(payload)).toString("base64url");
    return `${header}.${body}.sig`;
  }

  it("local-first issuer → authMode 'local-first' and no synthetic claim", () => {
    const session = resolveJwtSession(jwt({ sub: "u1", iss: LOCAL_FIRST_JWT_ISSUER }))!;
    expect(session.authMode).toBe("local-first");
    expect(session.claims.auth_mode).toBeUndefined();
  });

  it("anonymous issuer → authMode 'anonymous'", () => {
    const session = resolveJwtSession(jwt({ sub: "u1", iss: ANONYMOUS_JWT_ISSUER }))!;
    expect(session.authMode).toBe("anonymous");
    expect(session.claims.auth_mode).toBeUndefined();
  });

  it("any other issuer → authMode 'external'", () => {
    const session = resolveJwtSession(jwt({ sub: "u1", iss: "https://auth.example.com" }))!;
    expect(session.authMode).toBe("external");
    expect(session.claims.auth_mode).toBeUndefined();
  });
});
