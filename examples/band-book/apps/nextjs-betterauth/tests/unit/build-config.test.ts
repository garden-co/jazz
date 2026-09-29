import { describe, expect, it } from "vitest";
import {
  assertBuildConfiguration,
  readBuildConfig,
  usesLocalDefaults,
} from "../../src/lib/build-config.mjs";

const deployment = {
  NEXT_PUBLIC_APP_ORIGIN: "https://band.example",
  NEXT_PUBLIC_JAZZ_APP_ID: "band-book",
  NEXT_PUBLIC_JAZZ_SERVER_URL: "https://jazz.example",
};
const secrets = { BACKEND_SECRET: "b", BETTER_AUTH_SECRET: "a" };

describe("build config", () => {
  it("uses the checked-in development values only outside production", () => {
    expect(usesLocalDefaults(readBuildConfig({ NODE_ENV: "development" }))).toBe(true);
    expect(usesLocalDefaults(readBuildConfig({ NODE_ENV: "production" }))).toBe(false);
  });

  it("fails closed when a production deploy forgets its secrets", () => {
    expect(() =>
      assertBuildConfiguration(readBuildConfig({ NODE_ENV: "production", ...deployment })),
    ).toThrow(/missing: BACKEND_SECRET, BETTER_AUTH_SECRET$/);
    expect(
      assertBuildConfiguration(
        readBuildConfig({ NODE_ENV: "production", ...deployment, ...secrets }),
      ).backendSecret,
    ).toBe("b");
  });

  it("fails closed when a production deploy forgets its origin, app id or server URL", () => {
    // Otherwise the local origin would silently become the JWT issuer and audience.
    expect(() =>
      assertBuildConfiguration(readBuildConfig({ NODE_ENV: "production", ...secrets })),
    ).toThrow(
      /missing: NEXT_PUBLIC_APP_ORIGIN, NEXT_PUBLIC_JAZZ_APP_ID, NEXT_PUBLIC_JAZZ_SERVER_URL$/,
    );
    const { NEXT_PUBLIC_APP_ORIGIN: _origin, ...withoutOrigin } = deployment;
    expect(() =>
      assertBuildConfiguration(
        readBuildConfig({ NODE_ENV: "production", ...withoutOrigin, ...secrets }),
      ),
    ).toThrow(/missing: NEXT_PUBLIC_APP_ORIGIN$/);
  });

  it("requires secrets for any nonlocal deployment", () => {
    expect(() =>
      assertBuildConfiguration(readBuildConfig({ NEXT_PUBLIC_APP_ORIGIN: "https://band.example" })),
    ).toThrow(/missing/);
  });
});
