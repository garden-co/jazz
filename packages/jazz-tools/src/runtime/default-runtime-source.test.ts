import { describe, expect, it } from "vitest";
import {
  ANONYMOUS_JWT_ISSUER,
  LOCAL_FIRST_JWT_ISSUER,
  STATIC_BEARER_SESSION_ISSUER,
  markTrustedReservedSession,
  sessionFromVerifiedReservedJwtPayload,
} from "./client-session.js";
import { canonicalAuthorSubject } from "./author-id.js";
import { selfSignedClientProofFromConfig } from "./default-runtime-source.js";

describe("selfSignedClientProofFromConfig", () => {
  it("binds local-first and anonymous runtime opens to their signed author", () => {
    for (const [issuer, authMode] of [
      [LOCAL_FIRST_JWT_ISSUER, "local-first"],
      [ANONYMOUS_JWT_ISSUER, "anonymous"],
    ] as const) {
      const session = sessionFromVerifiedReservedJwtPayload(
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
      author: canonicalAuthorSubject(STATIC_BEARER_SESSION_ISSUER, "server"),
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
          author: canonicalAuthorSubject("https://issuer.example", "alice"),
          claims: {},
          authMode: "external",
        },
      ),
    ).toBeUndefined();
  });
});
