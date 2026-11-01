import { mintLocalFirstToken, verifyLocalFirstIdentityProof } from "jazz-napi";
import { describe, expect, it } from "vitest";
import { authSecretSeedForMinting } from "./auth-secret-codec.js";
import { generateAuthSecret } from "./auth-secret-store.js";
import { mintLocalFirstTokenFromSeed, verifyLocalFirstToken } from "./local-first-token.js";

// The native runtimes minted local-first tokens until alpha.60. Tokens from
// @garden-co/local-first-auth must stay interchangeable with theirs, so
// existing identities keep working against every core.
describe("local-first tokens from @garden-co/local-first-auth", () => {
  const apps = [
    "6b0c3f8e-2d9a-4f61-9a57-1c2e3d4f5a6b",
    "6B0C3F8E-2D9A-4F61-9A57-1C2E3D4F5A6B",
    "chat-app",
  ];

  it.each(apps)("are byte-identical to native tokens for app %s", (appId) => {
    const seed = authSecretSeedForMinting(generateAuthSecret());
    const now = Math.floor(Date.now() / 1000);
    const native = mintLocalFirstToken(seed, appId, 3600);
    const nativeIat = JSON.parse(atob(native.split(".")[1]!.replace(/-/g, "+").replace(/_/g, "/")))
      .iat as number;
    expect(Math.abs(nativeIat - now)).toBeLessThanOrEqual(1);
    expect(mintLocalFirstTokenFromSeed(seed, appId, 3600, nativeIat)).toBe(native);
  });

  it.each(apps)("verify the same way as the native verifier for app %s", (appId) => {
    const seed = authSecretSeedForMinting(generateAuthSecret());
    const token = mintLocalFirstTokenFromSeed(seed, appId, 3600);
    const native = verifyLocalFirstIdentityProof(token, appId);
    expect(native.ok).toBe(true);
    expect(verifyLocalFirstToken(token, appId)).toBe(native.id);
    expect(() => verifyLocalFirstToken(token, "another-app")).toThrow(/audience/);
    expect(verifyLocalFirstIdentityProof(token, "another-app").ok).toBe(false);
  });
});
