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
  internalSessionFromVerifiedReservedJwtPayload,
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
      session: {
        user: '["https://issuer.example","cookie-user"]',
        claims: { ...session.claims, iss: session.issuer, sub: session.user_id },
        authMode: "external",
      },
      internalSession: session,
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

  it("derives the user from iss/sub and exposes flat Better Auth-style metadata", () => {
    const jwt = makeJwt({
      sub: "user-subject",
      iss: "https://issuer.example",
      better_auth_user_id: "user-subject",
      profile_id: "profile-456",
    });

    const session = resolveClientSessionSync({
      appId: "app-jwt-sub",
      jwtToken: jwt,
    });

    expect(session).toEqual({
      user: '["https://issuer.example","user-subject"]',
      claims: {
        better_auth_user_id: "user-subject",
        profile_id: "profile-456",
        iss: "https://issuer.example",
        sub: "user-subject",
      },
      authMode: "external",
    });
  });

  it("keeps app metadata flat while reserved transport claims determine identity", () => {
    const metadata = {
      subject: "application-owned-subject",
      issuer: "application-owned-issuer",
      role: "editor",
    };

    expect(
      resolveClientSessionSync({
        appId: "app-exact-claims",
        jwtToken: makeJwt({ iss: "https://issuer.example", sub: "alice", ...metadata }),
      }),
    ).toMatchObject({
      user: '["https://issuer.example","alice"]',
      claims: { ...metadata, iss: "https://issuer.example", sub: "alice" },
    });
  });

  it("publishes an independent deeply immutable session without transport fields", () => {
    const providerClaims = {
      roles: ["writer"],
    };
    const session = resolveClientSessionSync({
      appId: "public-session-boundary",
      jwtToken: makeJwt({
        iss: "https://issuer.example",
        sub: "verified-subject",
        ...providerClaims,
      }),
    })!;

    providerClaims.roles.push("admin");
    expect(session).toEqual({
      user: '["https://issuer.example","verified-subject"]',
      claims: {
        iss: "https://issuer.example",
        sub: "verified-subject",
        roles: ["writer"],
      },
      authMode: "external",
    });
    expect(Object.isFrozen(session)).toBe(true);
    expect(Object.isFrozen(session.claims)).toBe(true);
    expect(Object.isFrozen(session.claims.roles)).toBe(true);
    for (const transportField of ["issuer", "user_id", "userId", "author"]) {
      expect(session).not.toHaveProperty(transportField);
    }
  });

  it("exposes every non-reserved top-level JSON claim without a nested-claims path", () => {
    const session = resolveClientSessionSync({
      appId: "app-jwt-policy-claim-corpus",
      jwtToken: makeJwt({
        iss: "https://issuer.example",
        sub: "alice",
        role: "top-level",
        issuer: "custom-provider-issuer",
        flags: ["writer", "beta"],
        profile: { id: "profile-456", active: true },
        revoked_at: null,
        // This spelling is ordinary app metadata; Jazz never flattens it.
        claims: { role: "nested" },
      }),
    });

    expect(session).toMatchObject({
      claims: {
        role: "top-level",
        issuer: "custom-provider-issuer",
        flags: ["writer", "beta"],
        profile: { id: "profile-456", active: true },
        revoked_at: null,
        claims: { role: "nested" },
        iss: "https://issuer.example",
        sub: "alice",
      },
    });
    expect(session?.claims.exp).toBeUndefined();
    expect(session?.claims.aud).toBeUndefined();
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

    expect(spaced?.claims.iss).toBe(" issuer ");
    expect(spaced?.claims.sub).toBe(" alice ");
    expect(spaced?.claims.subject).toBeUndefined();
    expect(spaced?.claims.iss).not.toBe(plain?.claims.iss);
    expect(spaced?.claims.sub).not.toBe(plain?.claims.sub);
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

      expect(session?.claims.iss).toBe(`${subject}issuer`);
      expect(session?.claims.sub).toBe(subject);
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
    ).toMatchObject({
      user: '["issuer🚀","alice🚀"]',
      claims: { iss: "issuer🚀", sub: "alice🚀" },
    });
  });

  it("rejects a JWT without an iss claim", () => {
    const jwt = makeJwt({
      sub: "user-subject",
      team: "eng",
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
      user: `["https://issuer.example","${SYSTEM_SESSION_ISSUER}"]`,
      claims: { iss: "https://issuer.example", sub: SYSTEM_SESSION_ISSUER },
      authMode: "external",
    });
  });

  it("returns null when no auth is configured", () => {
    expect(resolveClientSessionSync({ appId: "no-auth" })).toBeNull();
    expect(resolveClientSessionStateSync({ appId: "no-auth" })).toEqual({
      transport: null,
      session: null,
      internalSession: null,
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
      { sub: "u1", iss: LOCAL_FIRST_JWT_ISSUER, role: "writer" },
      "local-first",
    );
    expect(localFirst).toEqual({
      user: '["urn:jazz:local-first","u1"]',
      claims: { role: "writer", iss: LOCAL_FIRST_JWT_ISSUER, sub: "u1" },
      authMode: "local-first",
    });
    expect(
      resolveClientSessionSync({
        appId: "app-verified-local-first",
        jwtToken: jwt({ sub: "u1", iss: LOCAL_FIRST_JWT_ISSUER }),
        trustedReservedSession: internalSessionFromVerifiedReservedJwtPayload(
          { sub: "u1", iss: LOCAL_FIRST_JWT_ISSUER, role: "writer" },
          "local-first",
        )!,
      }),
    ).toEqual(localFirst);
    expect(
      sessionFromVerifiedReservedJwtPayload({ sub: "u1", iss: ANONYMOUS_JWT_ISSUER }, "anonymous"),
    ).toMatchObject({
      user: '["urn:jazz:anonymous","u1"]',
      claims: { iss: ANONYMOUS_JWT_ISSUER, sub: "u1" },
      authMode: "anonymous",
    });
    expect(
      sessionFromVerifiedReservedJwtPayload(
        { sub: "u1", iss: LOCAL_FIRST_JWT_ISSUER },
        "anonymous",
      ),
    ).toBeNull();
  });

  it("does not expose reserved self-signed proof keys as policy claims", () => {
    for (const [authMode, issuer] of [
      ["anonymous", ANONYMOUS_JWT_ISSUER],
      ["local-first", LOCAL_FIRST_JWT_ISSUER],
    ] as const) {
      for (const proofKey of ["first-proof-key", "second-proof-key"]) {
        const payload = { sub: "user", iss: issuer, jazz_pub_key: proofKey };
        expect(sessionFromVerifiedReservedJwtPayload(payload, authMode)).toEqual({
          user: JSON.stringify([issuer, "user"]),
          claims: { iss: issuer, sub: "user" },
          authMode,
        });
        expect(internalSessionFromVerifiedReservedJwtPayload(payload, authMode)?.claims).toEqual(
          {},
        );
      }
    }
    // An external provider may legitimately use this spelling as a custom
    // policy claim; only Jazz's dedicated proof format reserves the field.
    expect(
      resolveJwtSession(
        jwt({ iss: "https://auth.example.com", sub: "user", jazz_pub_key: "custom" }),
      )?.claims.jazz_pub_key,
    ).toBe("custom");
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
