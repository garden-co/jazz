import { describe, expect, it } from "vitest";
import {
  LOCAL_DEFAULTS,
  assertBuildConfiguration,
  readBuildConfig,
} from "../../src/lib/build-config.mjs";

const deployment = {
  NEXT_PUBLIC_APP_ORIGIN: "https://poster-shop.example",
  NEXT_PUBLIC_JAZZ_APP_ID: "8f0b7a52-5d0e-4b4c-9a51-3c1f2b9d7e10",
  BETTER_AUTH_SECRET: "better-auth-secret",
};
const remote = {
  JAZZ_SERVER_URL: "https://jazz.example",
  JAZZ_ADMIN_SECRET: "admin-secret",
  BACKEND_SECRET: "backend-secret",
};

describe("PosterShop build configuration", () => {
  it("lets the checked-in local setup use local fallbacks and a local store", () => {
    expect(assertBuildConfiguration(readBuildConfig({}))).toMatchObject({
      origin: LOCAL_DEFAULTS.origin,
      appId: LOCAL_DEFAULTS.appId,
      remote: undefined,
      dataPath: ".jazz",
    });
  });

  it.each([
    ["origin", { NEXT_PUBLIC_APP_ORIGIN: "https://poster-shop.example" }],
    ["app id", { NEXT_PUBLIC_JAZZ_APP_ID: deployment.NEXT_PUBLIC_JAZZ_APP_ID }],
    ["sync server", remote],
  ])("requires a Better Auth secret once the %s is not local", (_dimension, override) => {
    expect(() => assertBuildConfiguration(readBuildConfig(override))).toThrow(
      "must set BETTER_AUTH_SECRET",
    );
  });

  it.each(["JAZZ_ADMIN_SECRET", "BACKEND_SECRET"])("rejects a sync server without %s", (name) => {
    expect(() =>
      assertBuildConfiguration(readBuildConfig({ ...deployment, ...remote, [name]: undefined })),
    ).toThrow(`must set ${name}`);
  });

  it("accepts a configured deployment, with or without a sync server", () => {
    expect(assertBuildConfiguration(readBuildConfig(deployment))).toMatchObject({
      origin: "https://poster-shop.example",
      appId: deployment.NEXT_PUBLIC_JAZZ_APP_ID,
      remote: undefined,
    });
    expect(assertBuildConfiguration(readBuildConfig({ ...deployment, ...remote }))).toMatchObject({
      remote: { url: "https://jazz.example", adminSecret: "admin-secret" },
    });
  });
});
