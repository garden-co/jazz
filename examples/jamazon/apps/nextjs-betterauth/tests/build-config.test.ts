import { describe, expect, it } from "vitest";
import {
  assertBuildConfiguration,
  paymentProvider,
  readBuildConfig,
  usesLocalDefaults,
} from "../src/lib/build-config.mjs";

const secrets = { BETTER_AUTH_SECRET: "auth-secret" };
const deployment = {
  NODE_ENV: "production",
  NEXT_PUBLIC_APP_ORIGIN: "https://shop.example",
  NEXT_PUBLIC_JAZZ_APP_ID: "jamazon",
  PAYMENT_PROVIDER: "sandbox",
  ...secrets,
};
const check = (env: Record<string, string | undefined>) =>
  assertBuildConfiguration(readBuildConfig(env));

describe("build configuration fails closed", () => {
  it("treats a production process as a deployment even without an origin", () => {
    const config = readBuildConfig({ NODE_ENV: "production", ...secrets });
    expect(usesLocalDefaults(config)).toBe(false);
    expect(() => assertBuildConfiguration(config)).toThrow(
      /must set NEXT_PUBLIC_APP_ORIGIN, NEXT_PUBLIC_JAZZ_APP_ID, PAYMENT_PROVIDER/,
    );
  });

  it("treats a production process on a loopback origin as a deployment", () => {
    const env = { ...deployment, NEXT_PUBLIC_APP_ORIGIN: "http://127.0.0.1:3000" };
    expect(usesLocalDefaults(readBuildConfig(env))).toBe(false);
    expect(() => check({ ...env, PAYMENT_PROVIDER: undefined })).toThrow(
      /must set PAYMENT_PROVIDER/,
    );
  });

  it("never falls back to built-in secrets, locally or deployed", () => {
    expect(() => check({ NODE_ENV: "development" })).toThrow(/missing BETTER_AUTH_SECRET/);
    expect(() => check({ ...deployment, BETTER_AUTH_SECRET: "" })).toThrow(
      /must set BETTER_AUTH_SECRET/,
    );
  });

  it("does not treat a remote origin as local outside production", () => {
    const config = readBuildConfig({ ...secrets, NEXT_PUBLIC_APP_ORIGIN: "https://shop.example" });
    expect(usesLocalDefaults(config)).toBe(false);
    expect(() => paymentProvider(config)).toThrow(/must set PAYMENT_PROVIDER/);
  });

  it("accepts a fully configured deployment", () => {
    const config = check(deployment);
    expect(usesLocalDefaults(config)).toBe(false);
    expect(config.origin).toBe("https://shop.example");
  });

  it("uses the local defaults for a development run with generated secrets", () => {
    const config = check({ NODE_ENV: "development", ...secrets });
    expect(usesLocalDefaults(config)).toBe(true);
    expect(paymentProvider(config)).toBe("sandbox");
  });

  it("needs both of the app's secrets to sync through a remote server", () => {
    expect(() => check({ ...deployment, JAZZ_SERVER_URL: "https://sync.example" })).toThrow(
      /must set JAZZ_ADMIN_SECRET, BACKEND_SECRET/,
    );
    const config = check({
      ...deployment,
      JAZZ_SERVER_URL: "https://sync.example",
      JAZZ_ADMIN_SECRET: "admin",
      BACKEND_SECRET: "backend",
    });
    expect(config.remote).toEqual({
      url: "https://sync.example",
      adminSecret: "admin",
      backendSecret: "backend",
    });
  });

  it("keeps data in local SQLite unless a sync server is named", () => {
    const config = check({ NODE_ENV: "development", ...secrets });
    expect(config.remote).toBeUndefined();
    expect(config.dataPath).toBe(".jazz");
  });

  it("refuses live Stripe keys", () => {
    expect(() =>
      check({
        ...deployment,
        PAYMENT_PROVIDER: "stripe",
        STRIPE_SECRET_KEY: "sk_live_x",
        NEXT_PUBLIC_STRIPE_PUBLISHABLE_KEY: "pk_live_x",
      }),
    ).toThrow(/test-mode STRIPE_SECRET_KEY/);
  });
});
