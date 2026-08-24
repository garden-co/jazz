import { describe, expect, it } from "vitest";
import { resolveBandChatAuthSecret } from "./auth-secret.js";

describe("BandChat Better Auth secret", () => {
  it("fails closed with an actionable error when production has no secret", () => {
    expect(() => resolveBandChatAuthSecret({ NODE_ENV: "production" })).toThrow(
      /BETTER_AUTH_SECRET is required in production; set it in the deployment environment/,
    );
    expect(() =>
      resolveBandChatAuthSecret({ NODE_ENV: "production", BETTER_AUTH_SECRET: "   " }),
    ).toThrow(/BETTER_AUTH_SECRET is required in production/);
  });

  it("keeps the development scaffold usable and preserves configured secrets", () => {
    expect(resolveBandChatAuthSecret({ NODE_ENV: "development" })).toBe(
      "band-chat-development-secret",
    );
    expect(
      resolveBandChatAuthSecret({
        NODE_ENV: "production",
        BETTER_AUTH_SECRET: "deployment-secret",
      }),
    ).toBe("deployment-secret");
  });
});
