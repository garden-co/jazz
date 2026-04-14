import { afterEach, describe, expect, it, vi } from "vitest";
import { writeFile } from "node:fs/promises";
import { join } from "node:path";
import { createTempRootTracker, getAvailablePort, todoSchema } from "./test-helpers.js";
import * as devServer from "./dev-server.js";
import { __resetJazzNextPluginForTests, withJazz, type NextConfigLike } from "./next.js";

const dev = await import("./index.js");

const DEVELOPMENT_PHASE = "phase-development-server";
const PRODUCTION_BUILD_PHASE = "phase-production-build";

const tempRoots = createTempRootTracker();
const originalJazzServerUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL;
const originalJazzAppId = process.env.NEXT_PUBLIC_JAZZ_APP_ID;

async function resolveWrappedConfig(
  wrapped: ReturnType<typeof withJazz>,
  phase: string,
): Promise<NextConfigLike> {
  return wrapped(phase, { defaultConfig: {} });
}

afterEach(async () => {
  await __resetJazzNextPluginForTests();
  await tempRoots.cleanup();
  vi.restoreAllMocks();

  if (originalJazzServerUrl === undefined) {
    delete process.env.NEXT_PUBLIC_JAZZ_SERVER_URL;
  } else {
    process.env.NEXT_PUBLIC_JAZZ_SERVER_URL = originalJazzServerUrl;
  }

  if (originalJazzAppId === undefined) {
    delete process.env.NEXT_PUBLIC_JAZZ_APP_ID;
  } else {
    process.env.NEXT_PUBLIC_JAZZ_APP_ID = originalJazzAppId;
  }
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

  it("starts a local server in development and injects NEXT_PUBLIC_JAZZ_* env vars", async () => {
    const port = await getAvailablePort();
    const schemaDir = await tempRoots.create("jazz-next-test-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());

    const wrapped = withJazz(
      { reactStrictMode: true },
      {
        server: { port, adminSecret: "next-test-admin" },
        schemaDir,
      },
    );

    const resolved = await resolveWrappedConfig(wrapped, DEVELOPMENT_PHASE);

    const healthResponse = await fetch(`http://127.0.0.1:${port}/health`);
    expect(healthResponse.ok).toBe(true);

    const schemasResponse = await fetch(`http://127.0.0.1:${port}/schemas`, {
      headers: { "X-Jazz-Admin-Secret": "next-test-admin" },
    });
    expect(schemasResponse.ok).toBe(true);

    const body = (await schemasResponse.json()) as { hashes?: string[] };
    expect(body.hashes?.length).toBeGreaterThan(0);
    expect(resolved.env?.NEXT_PUBLIC_JAZZ_APP_ID).toBeTruthy();
    expect(resolved.env?.NEXT_PUBLIC_JAZZ_SERVER_URL).toBe(`http://127.0.0.1:${port}`);
    expect(process.env.NEXT_PUBLIC_JAZZ_APP_ID).toBe(resolved.env?.NEXT_PUBLIC_JAZZ_APP_ID);
    expect(process.env.NEXT_PUBLIC_JAZZ_SERVER_URL).toBe(`http://127.0.0.1:${port}`);
  }, 30_000);

  it("releases a failed startup before retrying the same port after the schema is fixed", async () => {
    const port = await getAvailablePort();
    const schemaDir = await tempRoots.create("jazz-next-retry-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());

    const pushSchemaCatalogue = vi
      .spyOn(devServer, "pushSchemaCatalogue")
      .mockRejectedValueOnce(new Error("schema push failed"));

    const wrapped = withJazz(
      {},
      {
        server: { port, adminSecret: "next-retry-admin" },
        schemaDir,
      },
    );

    await expect(resolveWrappedConfig(wrapped, DEVELOPMENT_PHASE)).rejects.toThrow(
      "schema push failed",
    );

    const resolved = await resolveWrappedConfig(wrapped, DEVELOPMENT_PHASE);

    expect(resolved.env?.NEXT_PUBLIC_JAZZ_SERVER_URL).toBe(`http://127.0.0.1:${port}`);
    expect(pushSchemaCatalogue).toHaveBeenCalledTimes(2);
  }, 30_000);

  it("throws when connecting to an existing server without adminSecret", async () => {
    process.env.NEXT_PUBLIC_JAZZ_SERVER_URL = "http://127.0.0.1:4000";
    process.env.NEXT_PUBLIC_JAZZ_APP_ID = "00000000-0000-0000-0000-000000000111";

    await expect(resolveWrappedConfig(withJazz({}), DEVELOPMENT_PHASE)).rejects.toThrow(
      "adminSecret is required when connecting to an existing server",
    );
  });

  it("throws when connecting to an existing server without appId", async () => {
    process.env.NEXT_PUBLIC_JAZZ_SERVER_URL = "http://127.0.0.1:4000";
    delete process.env.NEXT_PUBLIC_JAZZ_APP_ID;

    await expect(
      resolveWrappedConfig(withJazz({}, { adminSecret: "next-test-admin" }), DEVELOPMENT_PHASE),
    ).rejects.toThrow("appId is required when connecting to an existing server");
  });

  it("reuses the same managed server across repeated config resolution in one process", async () => {
    const port = await getAvailablePort();
    const schemaDir = await tempRoots.create("jazz-next-repeat-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());

    const wrapped = withJazz(
      {},
      {
        server: { port, adminSecret: "next-repeat-admin" },
        schemaDir,
      },
    );

    const first = await resolveWrappedConfig(wrapped, DEVELOPMENT_PHASE);
    const second = await resolveWrappedConfig(wrapped, DEVELOPMENT_PHASE);

    expect(first.env?.NEXT_PUBLIC_JAZZ_SERVER_URL).toBe(`http://127.0.0.1:${port}`);
    expect(second.env?.NEXT_PUBLIC_JAZZ_SERVER_URL).toBe(first.env?.NEXT_PUBLIC_JAZZ_SERVER_URL);
    expect(second.env?.NEXT_PUBLIC_JAZZ_APP_ID).toBe(first.env?.NEXT_PUBLIC_JAZZ_APP_ID);
  }, 30_000);

  it("throws on conflicting dev configurations in one process", async () => {
    const firstPort = await getAvailablePort();
    const firstSchemaDir = await tempRoots.create("jazz-next-conflict-a-");
    await writeFile(join(firstSchemaDir, "schema.ts"), todoSchema());

    const firstWrapped = withJazz(
      {},
      {
        server: { port: firstPort, adminSecret: "next-conflict-a" },
        schemaDir: firstSchemaDir,
      },
    );

    await resolveWrappedConfig(firstWrapped, DEVELOPMENT_PHASE);

    const secondPort = await getAvailablePort();
    const secondSchemaDir = await tempRoots.create("jazz-next-conflict-b-");
    await writeFile(join(secondSchemaDir, "schema.ts"), todoSchema());

    const secondWrapped = withJazz(
      {},
      {
        server: { port: secondPort, adminSecret: "next-conflict-b" },
        schemaDir: secondSchemaDir,
      },
    );

    await expect(resolveWrappedConfig(secondWrapped, DEVELOPMENT_PHASE)).rejects.toThrow(
      "conflicting Jazz dev runtime configuration",
    );
  }, 30_000);

  it("throws when env-driven existing-server config changes in one process", async () => {
    const schemaDir = await tempRoots.create("jazz-next-env-conflict-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());

    const serverHandle = await devServer.startLocalJazzServer({
      appId: "00000000-0000-0000-0000-000000000101",
      port: await getAvailablePort(),
      adminSecret: "next-env-conflict-admin",
    });

    try {
      process.env.NEXT_PUBLIC_JAZZ_SERVER_URL = serverHandle.url;
      process.env.NEXT_PUBLIC_JAZZ_APP_ID = serverHandle.appId;

      const wrapped = withJazz(
        {},
        {
          adminSecret: "next-env-conflict-admin",
          schemaDir,
        },
      );

      await resolveWrappedConfig(wrapped, DEVELOPMENT_PHASE);

      process.env.NEXT_PUBLIC_JAZZ_SERVER_URL = "http://127.0.0.1:59999";
      process.env.NEXT_PUBLIC_JAZZ_APP_ID = "00000000-0000-0000-0000-000000000202";

      await expect(resolveWrappedConfig(wrapped, DEVELOPMENT_PHASE)).rejects.toThrow(
        "conflicting Jazz dev runtime configuration",
      );
    } finally {
      await serverHandle.stop();
    }
  }, 30_000);
});

describe("dev barrel", () => {
  it("preserves the existing dev exports and exposes withJazz", () => {
    expect(dev.startLocalJazzServer).toBeDefined();
    expect(dev.pushSchemaCatalogue).toBeDefined();
    expect(dev.watchSchema).toBeDefined();
    expect(dev.jazzPlugin).toBeDefined();
    expect(dev.withJazz).toBe(withJazz);
  });
});
