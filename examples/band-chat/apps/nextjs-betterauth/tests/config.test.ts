import { describe, expect, it } from "vitest";
import {
  assertConfiguration,
  jazzServer,
  readConfig,
  usesLocalDefaults,
} from "../src/lib/config.mjs";

const secrets = { BACKEND_SECRET: "backend-secret", BETTER_AUTH_SECRET: "auth-secret" };
const deployment = {
  NODE_ENV: "production",
  NEXT_PUBLIC_APP_ORIGIN: "https://chat.example",
  NEXT_PUBLIC_JAZZ_APP_ID: "band-chat",
  NEXT_PUBLIC_JAZZ_SERVER_URL: "https://sync.example",
  ...secrets,
};
const check = (env: Record<string, string | undefined>) => assertConfiguration(readConfig(env));

describe("configuration fails closed", () => {
  it("treats a production process as a deployment even without an origin", () => {
    const config = readConfig({ NODE_ENV: "production", ...secrets });
    expect(usesLocalDefaults(config)).toBe(false);
    expect(() => assertConfiguration(config)).toThrow(
      /must set NEXT_PUBLIC_APP_ORIGIN, NEXT_PUBLIC_JAZZ_APP_ID, NEXT_PUBLIC_JAZZ_SERVER_URL/,
    );
  });

  it("treats a production process on a loopback origin as a deployment", () => {
    expect(() =>
      check({ ...deployment, NEXT_PUBLIC_APP_ORIGIN: "http://127.0.0.1:3000", BACKEND_SECRET: "" }),
    ).toThrow(/deployments must set BACKEND_SECRET/);
  });

  it("treats a non-loopback origin as a deployment in development too", () => {
    expect(() =>
      check({ NODE_ENV: "development", NEXT_PUBLIC_APP_ORIGIN: "https://chat.example", ...secrets }),
    ).toThrow(/deployments must set NEXT_PUBLIC_JAZZ_APP_ID, NEXT_PUBLIC_JAZZ_SERVER_URL/);
  });

  it("requires both secrets even for a local run, with no fallback", () => {
    expect(() => check({ NODE_ENV: "development" })).toThrow(
      /missing BACKEND_SECRET, BETTER_AUTH_SECRET\. Start it with `pnpm dev`/,
    );
    expect(() => check({ NODE_ENV: "development", BACKEND_SECRET: "x" })).toThrow(
      /missing BETTER_AUTH_SECRET/,
    );
  });

  it("accepts a local run with generated secrets on the loopback origin", () => {
    const config = check({ NODE_ENV: "development", ...secrets });
    expect(config.origin).toBe("http://127.0.0.1:3000");
    expect(config.betterAuthSecret).toBe("auth-secret");
  });

  it("requires the Jazz app and server before opening the auth backend", () => {
    expect(() => jazzServer(readConfig({ NODE_ENV: "development", ...secrets }))).toThrow(
      /NEXT_PUBLIC_JAZZ_APP_ID and NEXT_PUBLIC_JAZZ_SERVER_URL are not configured/,
    );
    expect(jazzServer(readConfig(deployment))).toEqual({
      appId: "band-chat",
      serverUrl: "https://sync.example",
    });
  });

  it("accepts a fully configured deployment", () => {
    expect(check(deployment).origin).toBe("https://chat.example");
  });
});
