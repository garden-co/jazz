import { describe, expect, it } from "vitest";
import type { Session } from "./context.js";
import {
  ANONYMOUS_JWT_ISSUER,
  LOCAL_FIRST_JWT_ISSUER,
  RESERVED_JAZZ_SESSION_ISSUERS,
  STATIC_BEARER_SESSION_ISSUER,
  SYSTEM_SESSION_ISSUER,
  resolveClientSessionSync,
  resolveClientSessionStateSync,
  resolveJwtSession,
  sessionFromVerifiedReservedJwtPayload,
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
      session: { ...session, user: '["https://issuer.example","cookie-user"]' },
    });
  });

  it("rejects malformed and externally supplied reserved cookie sessions", () => {
    for (const issuer of ["", " \t", ...RESERVED_JAZZ_SESSION_ISSUERS]) {
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
      user: '["https://issuer.example","user-subject"]',
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

  it("rejects unpaired surrogate issuer and subject components from JWTs and cookies", () => {
    for (const [iss, sub] of [
      ["issuer", "\ud800"],
      ["issuer", "\udc00"],
      ["\ud800", "alice"],
      ["\udc00", "alice"],
    ]) {
      expect(
        resolveClientSessionSync({
          appId: "app-jwt-surrogate-subject",
          jwtToken: makeJwt({ iss, sub }),
        }),
      ).toBeNull();
      expect(
        resolveClientSessionSync({
          appId: "cookie-app",
          cookieSession: {
            issuer: iss,
            user_id: sub,
            claims: {},
            authMode: "external",
          },
        }),
      ).toBeNull();
    }

    expect(
      resolveClientSessionSync({
        appId: "app-jwt-emoji-subject",
        jwtToken: makeJwt({ iss: "issuer🚀", sub: "alice🚀" }),
      }),
    ).toMatchObject({ issuer: "issuer🚀", user_id: "alice🚀" });
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

  it("rejects reserved issuers in generic JWT and cookie resolution", () => {
    for (const issuer of RESERVED_JAZZ_SESSION_ISSUERS) {
      expect(
        resolveClientSessionSync({
          appId: "app-reserved-spoof",
          jwtToken: makeJwt({ iss: issuer, sub: "user-controlled-subject" }),
        }),
      ).toBeNull();
      expect(
        resolveClientSessionSync({
          appId: "cookie-app",
          cookieSession: {
            issuer,
            user_id: "user-controlled-subject",
            claims: {},
            authMode:
              issuer === LOCAL_FIRST_JWT_ISSUER
                ? "local-first"
                : issuer === ANONYMOUS_JWT_ISSUER
                  ? "anonymous"
                  : "external",
          },
        }),
      ).toBeNull();
    }

    expect(
      resolveClientSessionSync({
        appId: "app-reserved-subject-only",
        jwtToken: makeJwt({ iss: "https://issuer.example", sub: SYSTEM_SESSION_ISSUER }),
      }),
    ).toMatchObject({
      issuer: "https://issuer.example",
      user_id: SYSTEM_SESSION_ISSUER,
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

describe("resolveJwtSession — reserved issuer admission", () => {
  function jwt(payload: Record<string, unknown>): string {
    const header = Buffer.from(JSON.stringify({ alg: "EdDSA", typ: "JWT" })).toString("base64url");
    const body = Buffer.from(JSON.stringify(payload)).toString("base64url");
    return `${header}.${body}.sig`;
  }

  it("generic JWT resolution rejects reserved Jazz issuers", () => {
    for (const issuer of [
      LOCAL_FIRST_JWT_ISSUER,
      ANONYMOUS_JWT_ISSUER,
      STATIC_BEARER_SESSION_ISSUER,
      SYSTEM_SESSION_ISSUER,
    ]) {
      expect(resolveJwtSession(jwt({ sub: "u1", iss: issuer }))).toBeNull();
    }
  });

  it("verified reserved JWT paths construct only their dedicated auth modes", () => {
    const localFirst = sessionFromVerifiedReservedJwtPayload(
      { sub: "u1", iss: LOCAL_FIRST_JWT_ISSUER, claims: { role: "writer" } },
      "local-first",
    );
    expect(localFirst).toEqual({
      issuer: LOCAL_FIRST_JWT_ISSUER,
      user_id: "u1",
      claims: { role: "writer" },
      authMode: "local-first",
      user: '["urn:jazz:local-first","u1"]',
    });
    expect(
      resolveClientSessionSync({
        appId: "app-verified-local-first",
        jwtToken: jwt({ sub: "u1", iss: LOCAL_FIRST_JWT_ISSUER }),
        trustedReservedSession: localFirst!,
      }),
    ).toBe(localFirst);
    expect(
      sessionFromVerifiedReservedJwtPayload({ sub: "u1", iss: ANONYMOUS_JWT_ISSUER }, "anonymous"),
    ).toMatchObject({ issuer: ANONYMOUS_JWT_ISSUER, user_id: "u1", authMode: "anonymous" });
    expect(
      sessionFromVerifiedReservedJwtPayload(
        { sub: "u1", iss: LOCAL_FIRST_JWT_ISSUER },
        "anonymous",
      ),
    ).toBeNull();
  });

  it("external issuer resolves as authMode 'external'", () => {
    const session = resolveJwtSession(jwt({ sub: "u1", iss: "https://auth.example.com" }))!;
    expect(session.authMode).toBe("external");
    expect(session.claims.auth_mode).toBeUndefined();
  });

  it("publishes the exact issuer-scoped user identity instead of a caller-provided alias", () => {
    const sameSubject = "provider-user";
    const issuerA = resolveClientSessionSync({
      appId: "author-a",
      cookieSession: {
        issuer: "https://issuer-a.example",
        user_id: sameSubject,
        claims: {},
        authMode: "external",
        // This is untyped hostile input at a public boundary. It must not be
        // preserved as the public user identity.
        user: "forged",
      } as Session,
    });
    const issuerB = resolveClientSessionSync({
      appId: "author-b",
      cookieSession: {
        issuer: "https://issuer-b.example",
        user_id: sameSubject,
        claims: {},
        authMode: "external",
      },
    });

    expect(issuerA?.user).toBe('["https://issuer-a.example","provider-user"]');
    expect(issuerB?.user).toBe('["https://issuer-b.example","provider-user"]');
    expect(issuerA?.user).not.toBe(issuerB?.user);
  });
});
