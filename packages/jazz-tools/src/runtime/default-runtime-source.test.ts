import { describe, expect, it } from "vitest";
import {
  ANONYMOUS_JWT_ISSUER,
  LOCAL_FIRST_JWT_ISSUER,
  STATIC_BEARER_SESSION_ISSUER,
  markTrustedReservedSession,
  internalSessionFromVerifiedReservedJwtPayload,
} from "./client-session.js";
import {
  selfSignedClientProofFromConfig,
  browserWorkerTransportAuth,
  trustAttachedBrowserWorkerSession,
} from "./default-runtime-source.js";
import { getTrustedReservedSession } from "./db-internal-session.js";
import type { DbConfig } from "./db.js";

function unsignedJwt(payload: Record<string, unknown>): string {
  const encode = (value: Record<string, unknown>) =>
    Buffer.from(JSON.stringify(value)).toString("base64url");
  return `${encode({ alg: "EdDSA", typ: "JWT" })}.${encode(payload)}.test-signature`;
}

describe("browserWorkerTransportAuth", () => {
  it("keeps a user's relay session separate from incidental deployment credentials", () => {
    expect(
      browserWorkerTransportAuth({
        appId: "relay-auth-app",
        jwtToken: "alice-session-token",
        adminSecret: "deployment-only-secret",
      }),
    ).toEqual({ jwt_token: "alice-session-token" });
    expect(
      browserWorkerTransportAuth({
        appId: "relay-auth-app",
        adminSecret: "deployment-only-secret",
      }),
    ).not.toHaveProperty("admin_secret");
  });
});

describe("selfSignedClientProofFromConfig", () => {
  it("binds local-first and anonymous runtime opens to their signed author", () => {
    for (const [issuer, authMode] of [
      [LOCAL_FIRST_JWT_ISSUER, "local-first"],
      [ANONYMOUS_JWT_ISSUER, "anonymous"],
    ] as const) {
      const session = internalSessionFromVerifiedReservedJwtPayload(
        { iss: issuer, sub: "alice" },
        authMode,
      )!;
      expect(
        selfSignedClientProofFromConfig(
          { appId: "proof-app", jwtToken: "self-signed-token" },
          session,
        ),
      ).toEqual({
        token: "self-signed-token",
        appId: "proof-app",
        claimedAuthor: JSON.stringify([issuer, "alice"]),
      });
    }
  });

  it("does not turn static or external sessions into native proof capabilities", () => {
    const staticBearer = markTrustedReservedSession({
      issuer: STATIC_BEARER_SESSION_ISSUER,
      user_id: "server",
      claims: {},
      authMode: "external",
    });
    expect(
      selfSignedClientProofFromConfig(
        { appId: "proof-app", jwtToken: "not-a-client-proof" },
        staticBearer,
      ),
    ).toBeUndefined();
    expect(
      selfSignedClientProofFromConfig(
        { appId: "proof-app", jwtToken: "external-token" },
        {
          issuer: "https://issuer.example",
          user_id: "alice",
          claims: {},
          authMode: "external",
        },
      ),
    ).toBeUndefined();
  });
});

describe("trustAttachedBrowserWorkerSession", () => {
  const session = {
    issuer: LOCAL_FIRST_JWT_ISSUER,
    user_id: "alice",
    claims: { role: "writer" },
    authMode: "local-first" as const,
  };

  function attachedConfig(
    overrides: { runtimeSources?: DbConfig["runtimeSources"] } = {},
  ): DbConfig {
    return {
      appId: "proof-app",
      jwtToken: unsignedJwt({
        iss: LOCAL_FIRST_JWT_ISSUER,
        sub: "alice",
        role: "writer",
      }),
      runtimeSources: {
        browserWorkerPort: {} as MessagePort,
        browserWorkerSession: session,
      },
      ...overrides,
    };
  }

  it("carries a matching reserved session across an attached worker boundary", () => {
    const config = attachedConfig();
    trustAttachedBrowserWorkerSession(config);

    expect(getTrustedReservedSession(config)).toEqual(session);
  });

  it("rejects a host session that does not match the attached identity token", () => {
    const config = attachedConfig({
      runtimeSources: {
        browserWorkerPort: {} as MessagePort,
        browserWorkerSession: { ...session, user_id: "mallory" },
      },
    });

    expect(() => trustAttachedBrowserWorkerSession(config)).toThrow(
      "Attached browser worker session does not match its identity token",
    );
  });

  it("takes attached claims from the identity token, not the forwarded session object", () => {
    const config = attachedConfig({
      runtimeSources: {
        browserWorkerPort: {} as MessagePort,
        browserWorkerSession: {
          ...session,
          claims: { role: "forged" },
        },
      },
    });

    trustAttachedBrowserWorkerSession(config);

    expect(getTrustedReservedSession(config)).toMatchObject({
      issuer: LOCAL_FIRST_JWT_ISSUER,
      user_id: "alice",
      authMode: "local-first",
      claims: { role: "writer" },
    });
  });

  it("does not trust forwarded session data without an attached worker port", () => {
    const config = attachedConfig({ runtimeSources: { browserWorkerSession: session } });
    trustAttachedBrowserWorkerSession(config);

    expect(getTrustedReservedSession(config)).toBeUndefined();
  });
});
