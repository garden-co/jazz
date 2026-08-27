import { describe, expect, it } from "vitest";
import {
  LOCAL_DEFAULTS,
  assertBuildConfiguration,
  readBuildConfig,
} from "../../src/lib/build-config.mjs";

const local = () => readBuildConfig({});
const configuredNonlocal = () =>
  readBuildConfig({
    NEXT_PUBLIC_APP_ORIGIN: "https://poster-shop.example",
    NEXT_PUBLIC_JAZZ_APP_ID: "poster-shop-production",
    NEXT_PUBLIC_JAZZ_SERVER_URL: "https://jazz.example",
    BACKEND_SECRET: "backend-secret",
    BETTER_AUTH_SECRET: "better-auth-secret",
  });

describe("PosterShop build configuration", () => {
  it("allows the exact checked-in local tuple to use local fallbacks", () => {
    expect(assertBuildConfiguration(local())).toMatchObject({
      origin: LOCAL_DEFAULTS.origin,
      appId: LOCAL_DEFAULTS.appId,
      serverUrl: LOCAL_DEFAULTS.serverUrl,
    });
  });

  it.each([
    ["origin", { NEXT_PUBLIC_APP_ORIGIN: "https://poster-shop.example" }],
    ["app id", { NEXT_PUBLIC_JAZZ_APP_ID: "poster-shop-production" }],
    ["server URL", { NEXT_PUBLIC_JAZZ_SERVER_URL: "https://jazz.example" }],
  ])("treats a nonlocal %s tuple dimension as secret-bearing", (_dimension, override) => {
    expect(() => assertBuildConfiguration(readBuildConfig(override))).toThrow(
      "BACKEND_SECRET and BETTER_AUTH_SECRET",
    );
  });

  it("rejects a nonlocal tuple missing BACKEND_SECRET", () => {
    expect(() =>
      assertBuildConfiguration(
        readBuildConfig({
          NEXT_PUBLIC_APP_ORIGIN: "https://poster-shop.example",
          NEXT_PUBLIC_JAZZ_APP_ID: "poster-shop-production",
          NEXT_PUBLIC_JAZZ_SERVER_URL: "https://jazz.example",
          BETTER_AUTH_SECRET: "better-auth-secret",
        }),
      ),
    ).toThrow("missing: BACKEND_SECRET");
  });

  it("rejects a nonlocal tuple missing BETTER_AUTH_SECRET", () => {
    expect(() =>
      assertBuildConfiguration(
        readBuildConfig({
          NEXT_PUBLIC_APP_ORIGIN: "https://poster-shop.example",
          NEXT_PUBLIC_JAZZ_APP_ID: "poster-shop-production",
          NEXT_PUBLIC_JAZZ_SERVER_URL: "https://jazz.example",
          BACKEND_SECRET: "backend-secret",
        }),
      ),
    ).toThrow("missing: BETTER_AUTH_SECRET");
  });

  it("accepts a fully configured nonlocal tuple", () => {
    expect(assertBuildConfiguration(configuredNonlocal())).toMatchObject({
      origin: "https://poster-shop.example",
      appId: "poster-shop-production",
      serverUrl: "https://jazz.example",
    });
  });
});
