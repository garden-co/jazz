import { describe, expect, it } from "vitest";
import {
  assertBuildConfiguration,
  readBuildConfig,
  usesLocalDefaults,
} from "../../src/lib/build-config.mjs";

const deployment = {
  NEXT_PUBLIC_APP_ORIGIN: "https://band.example",
  NEXT_PUBLIC_JAZZ_APP_ID: "4f8f6e0a-2c1b-4d7e-9f3a-5b6c7d8e9f01",
};
const remote = {
  JAZZ_SERVER_URL: "https://jazz.example",
  JAZZ_ADMIN_SECRET: "admin",
  BACKEND_SECRET: "backend",
};

describe("build config", () => {
  it("uses the checked-in development values only outside production", () => {
    expect(usesLocalDefaults(readBuildConfig({ NODE_ENV: "development" }))).toBe(true);
    expect(usesLocalDefaults(readBuildConfig({ NODE_ENV: "production" }))).toBe(false);
  });

  it("fails closed when a production deploy forgets its secret", () => {
    expect(() =>
      assertBuildConfiguration(readBuildConfig({ NODE_ENV: "production", ...deployment })),
    ).toThrow(/missing: BETTER_AUTH_SECRET$/);
    expect(
      assertBuildConfiguration(
        readBuildConfig({ NODE_ENV: "production", ...deployment, BETTER_AUTH_SECRET: "a" }),
      ).betterAuthSecret,
    ).toBe("a");
  });

  it("fails closed when a production deploy forgets its origin or app id", () => {
    // Otherwise the local origin would silently become Better Auth's origin.
    expect(() =>
      assertBuildConfiguration(
        readBuildConfig({ NODE_ENV: "production", BETTER_AUTH_SECRET: "a" }),
      ),
    ).toThrow(/missing: NEXT_PUBLIC_APP_ORIGIN, NEXT_PUBLIC_JAZZ_APP_ID$/);
  });

  it("requires both app secrets for a sync server", () => {
    const base = { ...deployment, BETTER_AUTH_SECRET: "a" };
    expect(assertBuildConfiguration(readBuildConfig({ ...base, ...remote })).remote).toEqual({
      url: "https://jazz.example",
      adminSecret: "admin",
      backendSecret: "backend",
    });
    expect(() =>
      assertBuildConfiguration(readBuildConfig({ ...base, ...remote, BACKEND_SECRET: undefined })),
    ).toThrow(/missing: BACKEND_SECRET$/);
  });

  it("requires the secret for any nonlocal deployment", () => {
    expect(() =>
      assertBuildConfiguration(readBuildConfig({ NEXT_PUBLIC_APP_ORIGIN: "https://band.example" })),
    ).toThrow(/missing: BETTER_AUTH_SECRET$/);
  });
});
