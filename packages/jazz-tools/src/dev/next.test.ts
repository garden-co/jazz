import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { createTempRootTracker, getAvailablePort, todoSchema } from "./test-helpers.js";
import * as devServer from "./dev-server.js";
import * as catalogueProject from "./catalogue-project.js";
import * as schemaWatcher from "./schema-watcher.js";
import { __resetJazzNextPluginForTests, withJazz, type NextConfigLike } from "./next.js";

const dev = await import("./index.js");

const DEVELOPMENT_PHASE = "phase-development-server";
const PRODUCTION_BUILD_PHASE = "phase-production-build";

const tempRoots = createTempRootTracker();
const originalJazzServerUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL;
const originalJazzAppId = process.env.NEXT_PUBLIC_JAZZ_APP_ID;
const originalBackendSecret = process.env.BACKEND_SECRET;
const originalJazzAdminSecret = process.env.JAZZ_ADMIN_SECRET;
async function resolveWrappedConfig(
  wrapped: ReturnType<typeof withJazz>,
  phase: string,
): Promise<NextConfigLike> {
  return wrapped(phase, { defaultConfig: {} });
}

function deployed(hash = "abc123def4567890") {
  return {
    schema: { hash, schemaFile: "schema.ts", status: "published" as const },
    warnings: [],
  };
}

// Managed-runtime writes NEXT_PUBLIC_JAZZ_APP_ID / NEXT_PUBLIC_JAZZ_SERVER_URL
// to process.env on successful init; that state leaks across vitest workers in
// the same thread pool and flips later tests onto the env-driven cloud branch.
// Scrub before each test so every case starts from the same baseline.
beforeEach(async () => {
  delete process.env.NEXT_PUBLIC_JAZZ_APP_ID;
  delete process.env.NEXT_PUBLIC_JAZZ_SERVER_URL;
  delete process.env.JAZZ_ADMIN_SECRET;
  delete process.env.BACKEND_SECRET;

  // Redirect cwd to a per-test directory for managed-runtime state.
  const fakeCwd = await tempRoots.create("jazz-next-test-cwd-");
  vi.spyOn(process, "cwd").mockReturnValue(fakeCwd);
});

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

  if (originalBackendSecret === undefined) {
    delete process.env.BACKEND_SECRET;
  } else {
    process.env.BACKEND_SECRET = originalBackendSecret;
  }

  if (originalJazzAdminSecret === undefined) {
    delete process.env.JAZZ_ADMIN_SECRET;
  } else {
    process.env.JAZZ_ADMIN_SECRET = originalJazzAdminSecret;
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
      expect.arrayContaining(["better-sqlite3", "jazz-napi"]),
    );
    expect(resolved.serverExternalPackages).not.toContain("jazz-tools");
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
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});

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

    const schemasResponse = await fetch(
      `http://127.0.0.1:${port}/apps/${resolved.env?.NEXT_PUBLIC_JAZZ_APP_ID}/schemas`,
      {
        headers: { "X-Jazz-Admin-Secret": "next-test-admin" },
      },
    );
    expect(schemasResponse.ok).toBe(true);

    const body = (await schemasResponse.json()) as { hashes?: string[] };
    expect(body.hashes?.length).toBeGreaterThan(0);
    expect(resolved.env?.NEXT_PUBLIC_JAZZ_APP_ID).toBeTruthy();
    expect(resolved.env?.NEXT_PUBLIC_JAZZ_SERVER_URL).toBe(`http://127.0.0.1:${port}`);
    expect(process.env.NEXT_PUBLIC_JAZZ_APP_ID).toBe(resolved.env?.NEXT_PUBLIC_JAZZ_APP_ID);
    expect(process.env.NEXT_PUBLIC_JAZZ_SERVER_URL).toBe(`http://127.0.0.1:${port}`);
    expect(logSpy).toHaveBeenCalledWith(
      expect.stringContaining(
        `Open the inspector: https://jazz2-inspector.vercel.app/#serverUrl=${encodeURIComponent(
          `http://127.0.0.1:${port}`,
        )}&appId=${encodeURIComponent(resolved.env?.NEXT_PUBLIC_JAZZ_APP_ID!)}&adminSecret=next-test-admin`,
      ),
    );
  }, 30_000);

  it("keeps a generated backend secret in the server process and out of returned Next config", async () => {
    vi.spyOn(devServer, "startLocalJazzServer")
      .mockResolvedValueOnce({
        appId: "00000000-0000-0000-0000-000000000181",
        port: 19881,
        url: "http://127.0.0.1:19881",
        dataDir: undefined as unknown as string,
        adminSecret: "next-secret-policy-admin-1",
        backendSecret: "generated-backend-canary-1",
        stop: vi.fn().mockResolvedValue(undefined),
      })
      .mockResolvedValueOnce({
        appId: "00000000-0000-0000-0000-000000000182",
        port: 19882,
        url: "http://127.0.0.1:19882",
        dataDir: undefined as unknown as string,
        adminSecret: "next-secret-policy-admin-2",
        backendSecret: "generated-backend-canary-2",
        stop: vi.fn().mockResolvedValue(undefined),
      });
    vi.spyOn(catalogueProject, "deploy").mockResolvedValue(deployed());
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const first = await resolveWrappedConfig(
      withJazz({}, { server: { adminSecret: "next-secret-policy-admin-1" } }),
      DEVELOPMENT_PHASE,
    );

    expect(process.env.BACKEND_SECRET).toBe("generated-backend-canary-1");
    expect(Object.hasOwn(first.env ?? {}, "BACKEND_SECRET")).toBe(false);
    expect(Object.values(first.env ?? {})).not.toContain("generated-backend-canary-1");
    expect(first.env?.NEXT_PUBLIC_JAZZ_APP_ID).toBeTruthy();
    expect(first.env?.NEXT_PUBLIC_JAZZ_SERVER_URL).toBe("http://127.0.0.1:19881");

    await __resetJazzNextPluginForTests();
    expect(process.env.BACKEND_SECRET).toBeUndefined();

    process.env.BACKEND_SECRET = "caller-owned-backend-secret";
    const second = await resolveWrappedConfig(
      withJazz({}, { server: { adminSecret: "next-secret-policy-admin-2" } }),
      DEVELOPMENT_PHASE,
    );

    expect(process.env.BACKEND_SECRET).toBe("generated-backend-canary-2");
    expect(Object.hasOwn(second.env ?? {}, "BACKEND_SECRET")).toBe(false);
    expect(Object.values(second.env ?? {})).not.toContain("generated-backend-canary-2");

    await __resetJazzNextPluginForTests();
    expect(process.env.BACKEND_SECRET).toBe("caller-owned-backend-secret");
  }, 30_000);

  it("releases a failed startup before retrying the same port after the schema is fixed", async () => {
    const port = await getAvailablePort();
    const schemaDir = await tempRoots.create("jazz-next-retry-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());

    const deploy = vi
      .spyOn(catalogueProject, "deploy")
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
    expect(deploy).toHaveBeenCalledTimes(2);
  }, 30_000);

  it("ignores a bare server URL env var with no adminSecret and starts a fresh local server", async () => {
    // Simulates the env being populated by a prior initialize() call in the
    // same process (Vite HMR restarts, repeated dev sessions in tests). A
    // bare env var is our own leftover, not a request to connect externally.
    process.env.NEXT_PUBLIC_JAZZ_SERVER_URL = "http://127.0.0.1:4000";
    process.env.NEXT_PUBLIC_JAZZ_APP_ID = "00000000-0000-0000-0000-000000000111";

    const schemaDir = await tempRoots.create("jazz-next-fallback-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());
    const port = await getAvailablePort();

    const resolved = await resolveWrappedConfig(
      withJazz({}, { server: { port }, schemaDir }),
      DEVELOPMENT_PHASE,
    );

    expect(resolved.env?.NEXT_PUBLIC_JAZZ_SERVER_URL).toBe(`http://127.0.0.1:${port}`);
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

  it("writes a dev schema-hash stub on startup and rewrites it on each schema push", async () => {
    vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000070",
      port: 19870,
      url: "http://127.0.0.1:19870",
      dataDir: undefined as unknown as string,
      adminSecret: "test-admin-secret",
      backendSecret: "test-backend-secret",
      stop: vi.fn().mockResolvedValue(undefined),
    });
    const deploy = vi
      .spyOn(catalogueProject, "deploy")
      .mockResolvedValue(deployed("1111111111111111aaaaaaaaaaaaaaaaaaaaaaaa"));
    let capturedOnPush: ((hash: string) => void) | undefined;
    vi.spyOn(schemaWatcher, "watchSchema").mockImplementation((opts) => {
      capturedOnPush = opts.onPush;
      return { close: vi.fn() };
    });

    const appRoot = await tempRoots.create("jazz-next-schema-hash-");
    const schemaDir = appRoot;
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());

    const wrapped = withJazz(
      {},
      {
        appRoot,
        schemaDir,
        server: { port: 19870, adminSecret: "next-schema-hash-admin" },
      },
    );

    await resolveWrappedConfig(wrapped, DEVELOPMENT_PHASE);

    const stubPath = join(appRoot, "node_modules", ".cache", "jazz", "schema-hash.js");
    const initial = await readFile(stubPath, "utf8");
    expect(initial).toContain("1111111111111111aaaaaaaaaaaaaaaaaaaaaaaa");

    expect(capturedOnPush).toBeDefined();
    await capturedOnPush!("2222222222222222bbbbbbbbbbbbbbbbbbbbbbbb");

    const updated = await readFile(stubPath, "utf8");
    expect(updated).toContain("2222222222222222bbbbbbbbbbbbbbbbbbbbbbbb");
    expect(updated).not.toContain("1111111111111111aaaaaaaaaaaaaaaaaaaaaaaa");

    expect(deploy).toHaveBeenCalled();
  });

  it("aliases jazz-tools/_dev/schema-hash to the generated stub for both webpack and turbopack", async () => {
    vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000080",
      port: 19880,
      url: "http://127.0.0.1:19880",
      dataDir: undefined as unknown as string,
      adminSecret: "test-admin-secret",
      backendSecret: "test-backend-secret",
      stop: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(catalogueProject, "deploy").mockResolvedValue(deployed("abc"));
    vi.spyOn(schemaWatcher, "watchSchema").mockImplementation(() => ({ close: vi.fn() }));

    const appRoot = await tempRoots.create("jazz-next-alias-");
    const schemaDir = appRoot;
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());

    const wrapped = withJazz(
      {},
      {
        appRoot,
        schemaDir,
        server: { port: 19880, adminSecret: "next-alias-admin" },
      },
    );

    const resolved = (await resolveWrappedConfig(wrapped, DEVELOPMENT_PHASE)) as NextConfigLike & {
      turbopack?: { resolveAlias?: Record<string, string> };
      webpack?: (config: { resolve?: { alias?: Record<string, string> } }) => unknown;
    };

    const expectedStub = join(appRoot, "node_modules", ".cache", "jazz", "schema-hash.js");

    expect(resolved.turbopack?.resolveAlias?.["jazz-tools/_dev/schema-hash"]).toBe(
      "./node_modules/.cache/jazz/schema-hash.js",
    );

    const baseConfig: { resolve?: { alias?: Record<string, string> } } = { resolve: { alias: {} } };
    const finalConfig = resolved.webpack!(baseConfig) as {
      resolve: { alias: Record<string, string> };
    };
    expect(finalConfig.resolve.alias["jazz-tools/_dev/schema-hash"]).toBe(expectedStub);
  });

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
    expect(dev.deploy).toBeDefined();
    expect(dev.watchSchema).toBeDefined();
    expect(dev.jazzPlugin).toBeDefined();
    expect(dev.withJazz).toBe(withJazz);
  });
});
