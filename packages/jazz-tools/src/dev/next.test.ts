import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { readFile, writeFile } from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
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
const originalCorrectnessRun = process.env.JAZZ_CORRECTNESS_ARTIFACT_RUN;
const originalCorrectnessWasmPackage = process.env.JAZZ_CORRECTNESS_WASM_PACKAGE;
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
    permissions: {
      schemaHash: hash,
      permissionsFile: "permissions.ts",
      previousHead: null,
      head: null,
    },
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
  delete process.env.JAZZ_CORRECTNESS_ARTIFACT_RUN;
  delete process.env.JAZZ_CORRECTNESS_WASM_PACKAGE;

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

  if (originalCorrectnessRun === undefined) delete process.env.JAZZ_CORRECTNESS_ARTIFACT_RUN;
  else process.env.JAZZ_CORRECTNESS_ARTIFACT_RUN = originalCorrectnessRun;
  if (originalCorrectnessWasmPackage === undefined)
    delete process.env.JAZZ_CORRECTNESS_WASM_PACKAGE;
  else process.env.JAZZ_CORRECTNESS_WASM_PACKAGE = originalCorrectnessWasmPackage;
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

  it("loads a workspace-linked jazz-napi through Node at runtime under Turbopack", async () => {
    // In this monorepo jazz-napi is a workspace link, which Turbopack will not
    // externalize via serverExternalPackages, so withJazz aliases it instead.
    const runtimeModule = fileURLToPath(new URL("./napi-runtime.js", import.meta.url));
    const fromProject = relative(process.cwd(), runtimeModule);
    const expectedAlias = fromProject.startsWith(".") ? fromProject : `./${fromProject}`;

    for (const phase of [PRODUCTION_BUILD_PHASE, DEVELOPMENT_PHASE]) {
      const resolved = (await resolveWrappedConfig(
        withJazz({ turbopack: { resolveAlias: { existing: "./existing" } } }, { server: false }),
        phase,
      )) as NextConfigLike & { turbopack?: { resolveAlias?: Record<string, string> } };

      expect(resolved.serverExternalPackages).toContain("jazz-napi");
      expect(resolved.turbopack?.resolveAlias).toEqual({
        existing: "./existing",
        "jazz-napi": expectedAlias,
      });
      expect(resolved.webpack).toBeUndefined();
    }
  });

  it("lets an app's own jazz-napi alias win over the workspace runtime alias", async () => {
    const resolved = (await resolveWrappedConfig(
      withJazz({ turbopack: { resolveAlias: { "jazz-napi": "./custom-napi.js" } } }),
      PRODUCTION_BUILD_PHASE,
    )) as NextConfigLike & { turbopack?: { resolveAlias?: Record<string, string> } };

    expect(resolved.turbopack?.resolveAlias?.["jazz-napi"]).toBe("./custom-napi.js");
  });

  it("publishes staged bytes atomically through the workspace native alias", async () => {
    const root = await tempRoots.create("jazz-next-native-upload-");
    const facade = join(root, "napi-runtime.mjs");
    const consumer = join(root, "consumer.mjs");
    await build({
      entryPoints: [fileURLToPath(new URL("./napi-runtime.ts", import.meta.url))],
      outfile: facade,
      format: "esm",
      platform: "node",
    });
    await build({
      stdin: {
        resolveDir: fileURLToPath(new URL(".", import.meta.url)),
        contents: `
          import assert from "node:assert/strict";
          import { randomBytes } from "node:crypto";
          import { NapiDb, StagedStreamingMutation } from ${JSON.stringify(facade)};
          import { schema as s } from "../index.js";
          import { encodeSchema } from "../runtime/native-runtime/schema-codec.js";
          import {
            openConfig, encodedCells, queryFromTable, PostcardReader, readNativeRowBatch,
          } from "../runtime/native-runtime/native-codec.js";
          import { rowsFromBatches } from "../runtime/native-runtime/native-runtime-adapter.js";
          import { createOpenTransactionId } from "../runtime/client.js";

          const app = s.defineApp({ files: s.table({ payload: s.bytes() }, {}) });
          const author = new TextEncoder().encode(JSON.stringify(["https://fixture.invalid", "fixture"]));
          const db = NapiDb.openMemoryAsBackend(encodeSchema(app.wasmSchema), openConfig(randomBytes(16), author, 1, true));
          const payload = Uint8Array.from({ length: 196731 }, (_, i) => (i * 29 + 7) & 255);
          const rowId = randomBytes(16);
          const idHex = rowId.toString("hex");
          const expectedId = [idHex.slice(0,8), idHex.slice(8,12), idHex.slice(12,16), idHex.slice(16,20), idHex.slice(20)].join("-");
          async function rows() {
            let result = db.all(queryFromTable("files"), { tier: "local", propagation: "local_only" });
            if (!(result instanceof Uint8Array)) {
              const pending = result;
              const deadline = Date.now() + 10000;
              while (!(result = pending.poll())) {
                assert.ok(Date.now() < deadline, "local native read settles");
                db.tick();
                await new Promise(resolve => setTimeout(resolve, 1));
              }
            }
            return rowsFromBatches(new PostcardReader(result).readVec(readNativeRowBatch), app.wasmSchema);
          }
          let write;
          const ticker = setInterval(() => db.tick(), 1);
          try {
            const upload = db.beginStreamingMutation("files", rowId, encodedCells([], []), "payload");
            for (let i = 0; i < payload.length; i += 16384) upload.push(payload.subarray(i, i + 16384));
            const staged = upload.stage();
            assert.ok(staged instanceof StagedStreamingMutation);
            assert.deepEqual(await rows(), []);
            const transaction = createOpenTransactionId();
            db.beginTransaction(transaction, "exclusive");
            staged.attach(transaction);
            assert.deepEqual(await rows(), []);
            write = db.commitTransaction(transaction, "exclusive");
            await write.wait("local");
            const published = await rows();
            assert.equal(published.length, 1);
            assert.equal(published[0].id, expectedId);
            assert.deepEqual(published[0].valuesByColumn.get("payload"), { type: "Bytea", value: payload });
            assert.throws(() => staged.attach(transaction));
            assert.equal(staged.abort(), false);
            assert.deepEqual(await rows(), published);
          } finally {
            clearInterval(ticker);
            write?.close();
            await db.close();
          }
        `,
      },
      outfile: consumer,
      bundle: true,
      packages: "external",
      external: [facade],
      format: "esm",
      platform: "node",
    });
    await promisify(execFile)(process.execPath, [consumer], {
      timeout: 20_000,
      env: {
        ...process.env,
        JAZZ_CORRECTNESS_ARTIFACT_RUN: originalCorrectnessRun,
        JAZZ_CORRECTNESS_WASM_PACKAGE: originalCorrectnessWasmPackage,
      },
    });
  }, 30_000);

  it("does not inject Jazz env vars outside the development phase", async () => {
    const resolved = await resolveWrappedConfig(withJazz({}), PRODUCTION_BUILD_PHASE);

    expect(resolved.env?.NEXT_PUBLIC_JAZZ_APP_ID).toBeUndefined();
    expect(resolved.env?.NEXT_PUBLIC_JAZZ_SERVER_URL).toBeUndefined();
    expect(resolved.env?.NEXT_PUBLIC_JAZZ_INSPECTOR).toBeUndefined();
    expect(resolved.rewrites).toBeUndefined();
    expect(process.env.NEXT_PUBLIC_JAZZ_APP_ID).toBeUndefined();
    expect(process.env.NEXT_PUBLIC_JAZZ_SERVER_URL).toBeUndefined();
  });

  it("routes both Next bundlers to the admitted WASM snapshot", async () => {
    process.env.JAZZ_CORRECTNESS_ARTIFACT_RUN = "1";
    process.env.JAZZ_CORRECTNESS_WASM_PACKAGE = "/sealed/wasm";
    const resolved = (await resolveWrappedConfig(
      withJazz({ turbopack: { resolveAlias: { existing: "./existing" } } }),
      PRODUCTION_BUILD_PHASE,
    )) as NextConfigLike & {
      turbopack?: { resolveAlias?: Record<string, string> };
      webpack?: (config: { resolve?: { alias?: Record<string, string> } }) => unknown;
    };

    expect(resolved.turbopack?.resolveAlias?.existing).toBe("./existing");
    const wasmEntry = resolve("/sealed/wasm", "jazz_wasm.js");
    const fromProject = relative(process.cwd(), wasmEntry);
    expect(resolved.turbopack?.resolveAlias?.["jazz-wasm"]).toBe(
      fromProject.startsWith(".") ? fromProject : `./${fromProject}`,
    );
    const finalConfig = resolved.webpack!({ resolve: { alias: {} } }) as {
      resolve: { alias: Record<string, string> };
    };
    expect(finalConfig.resolve.alias["jazz-wasm"]).toBe(wasmEntry);
  });

  it("fails before Next can resolve a mutable WASM package in a sealed run", async () => {
    process.env.JAZZ_CORRECTNESS_ARTIFACT_RUN = "1";
    await expect(resolveWrappedConfig(withJazz({}), PRODUCTION_BUILD_PHASE)).rejects.toThrow(
      "sealed correctness consumer is missing its admitted WASM package",
    );
  });

  it("starts a local server in development and injects NEXT_PUBLIC_JAZZ_* env vars", async () => {
    const schemaDir = await tempRoots.create("jazz-next-test-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());
    await writeFile(join(schemaDir, "permissions.ts"), "export default {};\n");
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});

    const wrapped = withJazz(
      { reactStrictMode: true },
      {
        server: { port: 0, adminSecret: "next-test-admin" },
        schemaDir,
      },
    );

    const resolved = await resolveWrappedConfig(wrapped, DEVELOPMENT_PHASE);

    const serverUrl = resolved.env?.NEXT_PUBLIC_JAZZ_SERVER_URL;
    expect(serverUrl).toMatch(/^http:\/\/127\.0\.0\.1:[1-9]\d*$/);

    const healthResponse = await fetch(`${serverUrl}/health`);
    expect(healthResponse.ok).toBe(true);

    const schemasResponse = await fetch(
      `${serverUrl}/apps/${resolved.env?.NEXT_PUBLIC_JAZZ_APP_ID}/schemas`,
      {
        headers: { "X-Jazz-Admin-Secret": "next-test-admin" },
      },
    );
    expect(schemasResponse.ok).toBe(true);

    const body = (await schemasResponse.json()) as { hashes?: string[] };
    expect(body.hashes?.length).toBeGreaterThan(0);
    expect(resolved.env?.NEXT_PUBLIC_JAZZ_APP_ID).toBeTruthy();
    expect(process.env.NEXT_PUBLIC_JAZZ_APP_ID).toBe(resolved.env?.NEXT_PUBLIC_JAZZ_APP_ID);
    expect(process.env.NEXT_PUBLIC_JAZZ_SERVER_URL).toBe(serverUrl);
    const inspectorLink = `https://jazz2-inspector.vercel.app/#serverUrl=${encodeURIComponent(
      serverUrl!,
    )}&appId=${encodeURIComponent(resolved.env?.NEXT_PUBLIC_JAZZ_APP_ID!)}`;
    expect(logSpy).toHaveBeenCalledWith(expect.stringContaining(inspectorLink));
    expect(logSpy.mock.calls.flat().join("\n")).not.toContain("next-test-admin");
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

  it("preserves an explicit empty backend secret instead of falling back to the environment", async () => {
    process.env.BACKEND_SECRET = "ambient-backend-secret";
    vi.spyOn(devServer, "startLocalJazzServer").mockImplementation(async (options) => {
      expect(options?.backendSecret).toBe("");
      throw new Error("backend secret must not be blank");
    });

    await expect(
      resolveWrappedConfig(
        withJazz({}, { server: { adminSecret: "next-empty-backend-secret", backendSecret: "" } }),
        DEVELOPMENT_PHASE,
      ),
    ).rejects.toThrow("backend secret must not be blank");
  });

  it("releases a failed startup before retrying the same port after the schema is fixed", async () => {
    const port = await getAvailablePort();
    const schemaDir = await tempRoots.create("jazz-next-retry-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());
    await writeFile(join(schemaDir, "permissions.ts"), "export default {};\n");

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
    await writeFile(join(schemaDir, "permissions.ts"), "export default {};\n");
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
    await writeFile(join(schemaDir, "permissions.ts"), "export default {};\n");

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
    await writeFile(join(firstSchemaDir, "permissions.ts"), "export default {};\n");

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
    await writeFile(join(secondSchemaDir, "permissions.ts"), "export default {};\n");

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
    await writeFile(join(schemaDir, "permissions.ts"), "export default {};\n");

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
    await writeFile(join(schemaDir, "permissions.ts"), "export default {};\n");

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
    await writeFile(join(schemaDir, "permissions.ts"), "export default {};\n");

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

  it("serves the inspector through a reusable loopback rewrite without replacing app routes", async () => {
    const schemaDir = await tempRoots.create("jazz-next-inspector-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());
    await writeFile(join(schemaDir, "permissions.ts"), "export default {};\n");
    const appRoutes = {
      beforeFiles: [{ source: "/api/:path*", destination: "http://localhost:3001/:path*" }],
      afterFiles: [{ source: "/old", destination: "/new" }],
      fallback: [{ source: "/:path*", destination: "/fallback/:path*" }],
    };
    const wrapped = withJazz(
      { rewrites: async () => appRoutes },
      { schemaDir, server: { inMemory: true } },
    );
    const resolved = await resolveWrappedConfig(wrapped, DEVELOPMENT_PHASE);
    const rewrites = await (resolved.rewrites as () => Promise<typeof appRoutes>)();
    expect(resolved.env?.NEXT_PUBLIC_JAZZ_INSPECTOR).toBe("1");
    expect(rewrites.beforeFiles.slice(1)).toEqual(appRoutes.beforeFiles);
    expect(rewrites.afterFiles).toEqual(appRoutes.afterFiles);
    expect(rewrites.fallback).toEqual(appRoutes.fallback);
    const route = rewrites.beforeFiles[0]!;
    expect(route.source).toBe("/__jazz/embedded/:path*");
    const url = route.destination.replace(":path*", "embedded.html");
    expect(new URL(url).hostname).toBe("127.0.0.1");
    const response = await fetch(url);
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toContain("text/html");
    expect(await response.text()).toContain("<script");

    const repeated = await resolveWrappedConfig(wrapped, DEVELOPMENT_PHASE);
    const repeatedRoutes = await (repeated.rewrites as () => Promise<typeof appRoutes>)();
    expect(repeatedRoutes.beforeFiles[0]).toEqual(route);
    await __resetJazzNextPluginForTests();
    await expect(fetch(url)).rejects.toThrow();
  }, 30_000);

  it("keeps array-form application rewrites in the afterFiles phase", async () => {
    const schemaDir = await tempRoots.create("jazz-next-inspector-array-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());
    await writeFile(join(schemaDir, "permissions.ts"), "export default {};\n");
    const appRoutes = [{ source: "/old", destination: "/new" }];
    const resolved = await resolveWrappedConfig(
      withJazz({ rewrites: async () => appRoutes }, { schemaDir, server: { inMemory: true } }),
      DEVELOPMENT_PHASE,
    );
    const routes = await (
      resolved.rewrites as () => Promise<{
        beforeFiles: typeof appRoutes;
        afterFiles: typeof appRoutes;
        fallback: typeof appRoutes;
      }>
    )();
    expect(routes.beforeFiles[0]?.source).toBe("/__jazz/embedded/:path*");
    expect(routes.afterFiles).toEqual(appRoutes);
    expect(routes.fallback).toEqual([]);
  }, 30_000);

  it("leaves inspector routing disabled when opted out", async () => {
    const schemaDir = await tempRoots.create("jazz-next-no-inspector-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());
    await writeFile(join(schemaDir, "permissions.ts"), "export default {};\n");
    const appRoutes = [{ source: "/old", destination: "/new" }];
    const resolved = await resolveWrappedConfig(
      withJazz(
        { rewrites: async () => appRoutes },
        { inspector: false, schemaDir, server: { inMemory: true } },
      ),
      DEVELOPMENT_PHASE,
    );
    expect(resolved.env?.NEXT_PUBLIC_JAZZ_INSPECTOR).toBeUndefined();
    expect(await (resolved.rewrites as () => Promise<typeof appRoutes>)()).toEqual(appRoutes);
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
