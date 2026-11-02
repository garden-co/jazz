import { describe, expect, it } from "vitest";
import { LOCAL_SECRETS, jazzStore, resolveConfig, resolveSecret } from "../src/lib/config.js";

describe("configuration fails closed", () => {
  it("uses local defaults for a development run", () => {
    const config = resolveConfig({ NODE_ENV: "development" });
    expect(config).toMatchObject({ appOrigin: "http://localhost:3000", isLocalOrigin: true });
    expect(resolveSecret(config, "BETTER_AUTH_SECRET", undefined)).toBe(
      LOCAL_SECRETS.BETTER_AUTH_SECRET,
    );
  });

  it("requires an origin in production", () => {
    expect(() => resolveConfig({ NODE_ENV: "production" })).toThrow(/NEXT_PUBLIC_APP_ORIGIN/);
  });

  it("never falls back to development secrets in production, even on a loopback origin", () => {
    const config = resolveConfig({
      NODE_ENV: "production",
      NEXT_PUBLIC_APP_ORIGIN: "http://localhost:3000",
    });
    expect(config.isLocalOrigin).toBe(false);
    expect(() => resolveSecret(config, "BETTER_AUTH_SECRET", undefined)).toThrow(
      /BETTER_AUTH_SECRET must be set/,
    );
    expect(resolveSecret(config, "BETTER_AUTH_SECRET", "configured")).toBe("configured");
  });

  it("treats a remote origin as a deployment outside production too", () => {
    const config = resolveConfig({
      NODE_ENV: "development",
      NEXT_PUBLIC_APP_ORIGIN: "https://warehouse.example",
    });
    expect(config.isLocalOrigin).toBe(false);
    expect(() => resolveSecret(config, "BETTER_AUTH_SECRET", "")).toThrow(
      /BETTER_AUTH_SECRET must be set/,
    );
  });

  it("keeps the data locally unless a sync server and both its secrets are set", () => {
    expect(jazzStore({})).toEqual({ kind: "sqlite", path: ".jazz" });
    const remote = { JAZZ_SERVER_URL: "https://jazz.example", JAZZ_ADMIN_SECRET: "a" };
    expect(() => jazzStore(remote)).toThrow(/JAZZ_ADMIN_SECRET and BACKEND_SECRET/);
    expect(jazzStore({ ...remote, BACKEND_SECRET: "b" })).toMatchObject({ kind: "remote" });
  });
});
