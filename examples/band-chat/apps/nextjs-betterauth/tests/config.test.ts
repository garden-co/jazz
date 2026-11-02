import { describe, expect, it } from "vitest";
import {
  assertConfiguration,
  LOCAL_APP_ID,
  readConfig,
  usesLocalDefaults,
} from "../src/lib/config.mjs";

const secrets = { BETTER_AUTH_SECRET: "auth-secret" };
const deployment = {
  NODE_ENV: "production",
  NEXT_PUBLIC_APP_ORIGIN: "https://chat.example",
  NEXT_PUBLIC_JAZZ_APP_ID: "band-chat",
  ...secrets,
};
const check = (env: Record<string, string | undefined>) => assertConfiguration(readConfig(env));

describe("configuration fails closed", () => {
  it("treats a production process as a deployment even without an origin", () => {
    const config = readConfig({ NODE_ENV: "production", ...secrets });
    expect(usesLocalDefaults(config)).toBe(false);
    expect(() => assertConfiguration(config)).toThrow(
      /must set NEXT_PUBLIC_APP_ORIGIN, NEXT_PUBLIC_JAZZ_APP_ID/,
    );
  });

  it("treats a production process on a loopback origin as a deployment", () => {
    expect(() =>
      check({
        ...deployment,
        NEXT_PUBLIC_APP_ORIGIN: "http://127.0.0.1:3000",
        BETTER_AUTH_SECRET: "",
      }),
    ).toThrow(/deployments must set BETTER_AUTH_SECRET/);
  });

  it("treats a non-loopback origin as a deployment in development too", () => {
    expect(() =>
      check({
        NODE_ENV: "development",
        NEXT_PUBLIC_APP_ORIGIN: "https://chat.example",
        ...secrets,
      }),
    ).toThrow(/deployments must set NEXT_PUBLIC_JAZZ_APP_ID/);
  });

  it("requires the Better Auth secret even for a local run, with no fallback", () => {
    expect(() => check({ NODE_ENV: "development" })).toThrow(
      /missing BETTER_AUTH_SECRET\. Start it with `pnpm dev`/,
    );
  });

  it("accepts a local run with local SQLite and a local app id", () => {
    const config = check({ NODE_ENV: "development", ...secrets });
    expect(config.origin).toBe("http://127.0.0.1:3000");
    expect(config.appId).toBe(LOCAL_APP_ID);
    expect(config.remote).toBeUndefined();
    expect(config.dataPath).toBe(".jazz");
  });

  it("needs both of the app's secrets to sync through a remote server", () => {
    expect(() => check({ ...deployment, JAZZ_SERVER_URL: "https://sync.example" })).toThrow(
      /must set JAZZ_ADMIN_SECRET, BACKEND_SECRET/,
    );
    expect(
      check({
        ...deployment,
        JAZZ_SERVER_URL: "https://sync.example",
        JAZZ_ADMIN_SECRET: "admin",
        BACKEND_SECRET: "backend",
      }).remote,
    ).toEqual({ url: "https://sync.example", adminSecret: "admin", backendSecret: "backend" });
  });

  it("accepts a fully configured deployment", () => {
    expect(check(deployment).origin).toBe("https://chat.example");
  });
});
