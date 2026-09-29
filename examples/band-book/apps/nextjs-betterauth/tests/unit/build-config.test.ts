import { describe, expect, it } from "vitest";
import {
  assertBuildConfiguration,
  readBuildConfig,
  usesLocalDefaults,
} from "../../src/lib/build-config.mjs";

describe("build config", () => {
  it("uses the checked-in development values only outside production", () => {
    expect(usesLocalDefaults(readBuildConfig({ NODE_ENV: "development" }))).toBe(true);
    expect(usesLocalDefaults(readBuildConfig({ NODE_ENV: "production" }))).toBe(false);
  });

  it("fails closed when a production deploy forgets its secrets", () => {
    expect(() => assertBuildConfiguration(readBuildConfig({ NODE_ENV: "production" }))).toThrow(
      /BACKEND_SECRET, BETTER_AUTH_SECRET/,
    );
    expect(
      assertBuildConfiguration(
        readBuildConfig({ NODE_ENV: "production", BACKEND_SECRET: "b", BETTER_AUTH_SECRET: "a" }),
      ).backendSecret,
    ).toBe("b");
  });

  it("requires secrets for any nonlocal deployment", () => {
    expect(() =>
      assertBuildConfiguration(readBuildConfig({ NEXT_PUBLIC_APP_ORIGIN: "https://band.example" })),
    ).toThrow(/missing/);
  });
});
