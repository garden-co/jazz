import { describe, expect, it } from "vitest";
import {
  assertBuildConfiguration,
  paymentProvider,
  readBuildConfig,
  usesLocalDefaults,
} from "../src/lib/build-config.mjs";

const secrets = { BACKEND_SECRET: "backend-secret", BETTER_AUTH_SECRET: "auth-secret" };
const deployment = {
  NODE_ENV: "production",
  NEXT_PUBLIC_APP_ORIGIN: "https://shop.example",
  NEXT_PUBLIC_JAZZ_APP_ID: "jamazon",
  NEXT_PUBLIC_JAZZ_SERVER_URL: "https://sync.example",
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
      /must set NEXT_PUBLIC_APP_ORIGIN, NEXT_PUBLIC_JAZZ_APP_ID, NEXT_PUBLIC_JAZZ_SERVER_URL, PAYMENT_PROVIDER/,
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
    expect(() => check({ NODE_ENV: "development" })).toThrow(
      /missing BACKEND_SECRET, BETTER_AUTH_SECRET/,
    );
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
