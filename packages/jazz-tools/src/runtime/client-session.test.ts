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
      user: '["https://issuer.example","user-subject"]',
      claims: { role: "editor", iss: "https://issuer.example", sub: "user-subject" },
      authMode: "external",
    });
  });

  it("preserves provider claims but overwrites spoofed iss/sub with verified identity", () => {
    const claims = {
      subject: "application-owned-subject",
      issuer: "application-owned-issuer",
      sub: "application-owned-sub",
      role: "editor",
    };

    expect(
      resolveClientSessionSync({
        appId: "app-exact-claims",
        jwtToken: makeJwt({ iss: "https://issuer.example", sub: "alice", claims }),
      }),
    ).toMatchObject({
      user: '["https://issuer.example","alice"]',
      claims: { ...claims, iss: "https://issuer.example", sub: "alice" },
    });
  });

  it("publishes an independent deeply immutable session without transport fields", () => {
    const providerClaims = {
      iss: "spoofed-issuer",
      sub: "spoofed-subject",
      roles: ["writer"],
    };
    const session = resolveClientSessionSync({
      appId: "public-session-boundary",
      jwtToken: makeJwt({
        iss: "https://issuer.example",
        sub: "verified-subject",
        claims: providerClaims,
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

  it("mirrors server JWT policy-claim admission, including deterministic collision precedence", () => {
    const session = resolveClientSessionSync({
      appId: "app-jwt-policy-claim-corpus",
      jwtToken: makeJwt({
        iss: "https://issuer.example",
        sub: "alice",
        // `claims` is visited before `role`; the later top-level custom claim
        // wins exactly as server admission's BTreeMap traversal does.
        claims: { role: "nested", issuer: "nested-issuer" },
        role: "top-level",
        issuer: "custom-provider-issuer",
        metadata: { intentionally: "not policy-visible" },
      }),
    });

    expect(session).toMatchObject({
      claims: {
        role: "top-level",
        issuer: "custom-provider-issuer",
        iss: "https://issuer.example",
        sub: "alice",
      },
    });
    expect(session?.claims.metadata).toBeUndefined();
  });

  it("rejects unsupported nested policy claims instead of diverging from server admission", () => {
    expect(
      resolveClientSessionSync({
        appId: "app-jwt-nested-policy-claim",
        jwtToken: makeJwt({
          iss: "https://issuer.example",
          sub: "alice",
          claims: { profile: { role: "writer" } },
        }),
      }),
    ).toBeNull();
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
      { sub: "u1", iss: LOCAL_FIRST_JWT_ISSUER, claims: { role: "writer" } },
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
          { sub: "u1", iss: LOCAL_FIRST_JWT_ISSUER, claims: { role: "writer" } },
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
