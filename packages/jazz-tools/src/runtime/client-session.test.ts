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
      issuer: "https://issuer.example",
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

  it("rejects malformed and externally supplied system cookie sessions", () => {
    for (const issuer of ["", " \t", "urn:jazz:system"]) {
      expect(
        resolveClientSessionSync({
          appId: "cookie-app",
          cookieSession: {
            issuer,
            user_id: "alice",
            claims: {},
            authMode: "external",
          },
        }),
      ).toBeNull();
    }
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
      issuer: "https://issuer.example",
      user_id: "user-subject",
      claims: { role: "editor" },
      authMode: "external",
    });
  });

  it("preserves the exact claims object without identity aliases", () => {
    const claims = {
      subject: "application-owned-subject",
      issuer: "application-owned-issuer",
      sub: "application-owned-sub",
      nested: { role: "editor" },
    };

    expect(
      resolveClientSessionSync({
        appId: "app-exact-claims",
        jwtToken: makeJwt({ iss: "https://issuer.example", sub: "alice", claims }),
      }),
    ).toMatchObject({ issuer: "https://issuer.example", user_id: "alice", claims });
  });

  it("preserves exact nonblank JWT issuer and subject bytes and rejects ASCII-whitespace-only components", () => {
    const spaced = resolveClientSessionSync({
      appId: "app-jwt-spaced-subject",
      jwtToken: makeJwt({ iss: " issuer ", sub: " alice " }),
    });
    const plain = resolveClientSessionSync({
      appId: "app-jwt-plain-subject",
      jwtToken: makeJwt({ iss: "issuer", sub: "alice" }),
    });

    expect(spaced?.issuer).toBe(" issuer ");
    expect(spaced?.user_id).toBe(" alice ");
    expect(spaced?.claims.subject).toBeUndefined();
    expect(spaced?.issuer).not.toBe(plain?.issuer);
    expect(spaced?.user_id).not.toBe(plain?.user_id);
    for (const subject of [" ", "\t", "\n", "\v", "\f", "\r", " \t\n\v\f\r "]) {
      expect(
        resolveClientSessionSync({
          appId: "app-jwt-whitespace-subject",
          jwtToken: makeJwt({ iss: "issuer", sub: subject }),
        }),
      ).toBeNull();
    }
    for (const issuer of [" ", "\t", "\n", "\v", "\f", "\r", " \t\n\v\f\r "]) {
      expect(
        resolveClientSessionSync({
          appId: "app-jwt-whitespace-issuer",
          jwtToken: makeJwt({ iss: issuer, sub: "alice" }),
        }),
      ).toBeNull();
    }
  });

  it("preserves Unicode whitespace in usable issuer and subject components", () => {
    for (const subject of ["\u0085", "\uFEFF", "\u0085provider", "provider\uFEFF"]) {
      const session = resolveClientSessionSync({
        appId: "app-jwt-unicode-subject",
        jwtToken: makeJwt({ iss: `${subject}issuer`, sub: subject }),
      });

      expect(session?.issuer).toBe(`${subject}issuer`);
      expect(session?.user_id).toBe(subject);
      expect(session?.claims.subject).toBeUndefined();
    }
  });

  it("rejects a JWT without an iss claim", () => {
    const jwt = makeJwt({
      sub: "user-subject",
      claims: { team: "eng" },
    });

    const session = resolveClientSessionSync({
      appId: "app-jwt-sub-only",
      jwtToken: jwt,
    });

    expect(session).toBeNull();
  });

  it("rejects externally supplied system identities", () => {
    expect(
      resolveClientSessionSync({
        appId: "app-system-spoof",
        jwtToken: makeJwt({ iss: "urn:jazz:system", sub: "system" }),
      }),
    ).toBeNull();
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
