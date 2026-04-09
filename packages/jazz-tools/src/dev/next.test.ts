import { afterEach, describe, expect, it } from "vitest";
import { __resetJazzNextPluginForTests, withJazz, type NextConfigLike } from "./next.js";

const DEVELOPMENT_PHASE = "phase-development-server";
const PRODUCTION_BUILD_PHASE = "phase-production-build";

async function resolveWrappedConfig(
  wrapped: ReturnType<typeof withJazz>,
  phase: string,
): Promise<NextConfigLike> {
  return wrapped(phase, { defaultConfig: {} });
}

afterEach(async () => {
  await __resetJazzNextPluginForTests();
  delete process.env.NEXT_PUBLIC_JAZZ_APP_ID;
  delete process.env.NEXT_PUBLIC_JAZZ_SERVER_URL;
});

describe("withJazz", () => {
  it("preserves existing config fields and unions serverExternalPackages", async () => {
    const resolved = await resolveWrappedConfig(
      withJazz({
        reactStrictMode: true,
        env: { EXISTING_ENV: "1" },
        serverExternalPackages: ["sharp", "jazz-tools"],
      }),
      PRODUCTION_BUILD_PHASE,
    );

    expect(resolved.reactStrictMode).toBe(true);
    expect(resolved.env).toEqual({ EXISTING_ENV: "1" });
    expect(resolved.serverExternalPackages).toEqual(
      expect.arrayContaining(["sharp", "jazz-tools", "jazz-napi"]),
    );
    expect(resolved.serverExternalPackages?.filter((value) => value === "jazz-tools")).toHaveLength(
      1,
    );
  });

  it("supports config functions as input", async () => {
    const resolved = await resolveWrappedConfig(
      withJazz(async () => ({
        poweredByHeader: false,
        serverExternalPackages: ["better-sqlite3"],
      })),
      PRODUCTION_BUILD_PHASE,
    );

    expect(resolved.poweredByHeader).toBe(false);
    expect(resolved.serverExternalPackages).toEqual(
      expect.arrayContaining(["better-sqlite3", "jazz-tools", "jazz-napi"]),
    );
  });

  it("does not inject Jazz env vars outside the development phase", async () => {
    const resolved = await resolveWrappedConfig(withJazz({}), PRODUCTION_BUILD_PHASE);

    expect(resolved.env?.NEXT_PUBLIC_JAZZ_APP_ID).toBeUndefined();
    expect(resolved.env?.NEXT_PUBLIC_JAZZ_SERVER_URL).toBeUndefined();
    expect(process.env.NEXT_PUBLIC_JAZZ_APP_ID).toBeUndefined();
    expect(process.env.NEXT_PUBLIC_JAZZ_SERVER_URL).toBeUndefined();
  });
});
