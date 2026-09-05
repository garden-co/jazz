import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { join } from "node:path";
import { promisify } from "node:util";
import { execFile as execFileCallback, spawn } from "node:child_process";
import { createTempRootTracker, todoSchema } from "./test-helpers.js";
import * as devServer from "./dev-server.js";
import * as catalogueProject from "./catalogue-project.js";
import * as schemaWatcher from "./schema-watcher.js";
import { ensureEnvAppId, ManagedDevRuntime } from "./managed-runtime.js";
import { chmod, lstat, readFile, stat, symlink, writeFile } from "node:fs/promises";
import { build } from "esbuild";
import { unlock, waitForLockSync } from "fs-native-extensions";
import {
  appendFileSync,
  closeSync,
  fsyncSync,
  openSync,
  renameSync,
  writeFileSync,
  writeSync,
} from "node:fs";

const execFile = promisify(execFileCallback);

const tempRoots = createTempRootTracker();

const originalJazzAppId = process.env.VITE_JAZZ_APP_ID;
const originalJazzServerUrl = process.env.VITE_JAZZ_SERVER_URL;
const originalAdminSecret = process.env.JAZZ_ADMIN_SECRET;
const originalBackendSecret = process.env.BACKEND_SECRET;
const originalStdoutIsTTYDescriptor = Object.getOwnPropertyDescriptor(process.stdout, "isTTY");

function makeRuntime(): ManagedDevRuntime {
  return new ManagedDevRuntime({
    appId: "VITE_JAZZ_APP_ID",
    serverUrl: "VITE_JAZZ_SERVER_URL",
    telemetryCollectorUrl: "VITE_JAZZ_TELEMETRY_COLLECTOR_URL",
  });
}

function makeFetchFailedError(code: string): TypeError & { cause?: unknown } {
  const error = new TypeError("fetch failed") as TypeError & { cause?: unknown };
  error.cause = Object.assign(new Error(`getaddrinfo ${code} v2.sync.jazz.tools`), {
    code,
    hostname: "v2.sync.jazz.tools",
  });
  return error;
}

function setStdoutIsTTY(isTTY: boolean): void {
  Object.defineProperty(process.stdout, "isTTY", {
    configurable: true,
    value: isTTY,
  });
}

function deployed(hash = "abc123def4567890") {
  return {
    schema: { hash, schemaFile: "schema.ts", status: "published" as const },
    warnings: [],
  };
}

beforeEach(() => {
  delete process.env.VITE_JAZZ_APP_ID;
  delete process.env.VITE_JAZZ_SERVER_URL;
  delete process.env.JAZZ_ADMIN_SECRET;
  delete process.env.BACKEND_SECRET;
});

afterEach(async () => {
  await tempRoots.cleanup();
  vi.restoreAllMocks();

  if (originalStdoutIsTTYDescriptor) {
    Object.defineProperty(process.stdout, "isTTY", originalStdoutIsTTYDescriptor);
  } else {
    delete (process.stdout as { isTTY?: boolean }).isTTY;
  }

  if (originalJazzAppId === undefined) {
    delete process.env.VITE_JAZZ_APP_ID;
  } else {
    process.env.VITE_JAZZ_APP_ID = originalJazzAppId;
  }

  if (originalJazzServerUrl === undefined) {
    delete process.env.VITE_JAZZ_SERVER_URL;
  } else {
    process.env.VITE_JAZZ_SERVER_URL = originalJazzServerUrl;
  }

  if (originalAdminSecret === undefined) {
    delete process.env.JAZZ_ADMIN_SECRET;
  } else {
    process.env.JAZZ_ADMIN_SECRET = originalAdminSecret;
  }

  if (originalBackendSecret === undefined) {
    delete process.env.BACKEND_SECRET;
  } else {
    process.env.BACKEND_SECRET = originalBackendSecret;
  }
});

describe("ManagedDevRuntime", () => {
  it("persists the app ID before starting the local server", async () => {
    const schemaDir = await tempRoots.create("jazz-managed-bootstrap-env-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());
    const appId = "00000000-0000-0000-0000-000000000124";
    let envAtServerStartup = "";

    vi.spyOn(devServer, "startLocalJazzServer").mockImplementation(async () => {
      envAtServerStartup = await readFile(join(schemaDir, ".env"), "utf8");
      return {
        appId,
        port: 19884,
        url: "http://127.0.0.1:19884",
        dataDir: join(schemaDir, "node_modules", ".cache", "jazz-dev-server"),
        adminSecret: "bootstrap-admin",
        backendSecret: "bootstrap-backend",
        stop: vi.fn().mockResolvedValue(undefined),
      };
    });
    vi.spyOn(catalogueProject, "deploy").mockResolvedValue(deployed());
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const runtime = makeRuntime();
    try {
      await runtime.initialize({
        appId,
        schemaDir,
        server: { port: 19884, adminSecret: "bootstrap-admin" },
      });

      expect(envAtServerStartup).toContain(`VITE_JAZZ_APP_ID=${appId}`);
    } finally {
      await runtime.dispose();
    }
  });

  it("restores the backend secret after stop failure and permits retry", async () => {
    const firstSchemaDir = await tempRoots.create("jazz-managed-stop-failure-first-");
    const secondSchemaDir = await tempRoots.create("jazz-managed-stop-failure-second-");
    await writeFile(join(firstSchemaDir, "schema.ts"), todoSchema());
    await writeFile(join(secondSchemaDir, "schema.ts"), todoSchema());
    const stopError = new Error("server stop failed");

    vi.spyOn(devServer, "startLocalJazzServer")
      .mockResolvedValueOnce({
        appId: "00000000-0000-0000-0000-000000000128",
        port: 19888,
        url: "http://127.0.0.1:19888",
        dataDir: undefined as unknown as string,
        adminSecret: "stop-failure-admin-1",
        backendSecret: "stop-failure-backend-1",
        stop: vi.fn().mockRejectedValue(stopError),
      })
      .mockResolvedValueOnce({
        appId: "00000000-0000-0000-0000-000000000129",
        port: 19889,
        url: "http://127.0.0.1:19889",
        dataDir: undefined as unknown as string,
        adminSecret: "stop-failure-admin-2",
        backendSecret: "stop-failure-backend-2",
        stop: vi.fn().mockResolvedValue(undefined),
      });
    vi.spyOn(catalogueProject, "deploy").mockResolvedValue(deployed());
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const runtime = makeRuntime();
    await runtime.initialize({
      schemaDir: firstSchemaDir,
      server: { port: 19888, adminSecret: "stop-failure-admin-1" },
    });
    expect(process.env.BACKEND_SECRET).toBe("stop-failure-backend-1");

    await expect(runtime.dispose()).rejects.toBe(stopError);
    expect(process.env.BACKEND_SECRET).toBeUndefined();

    await runtime.initialize({
      schemaDir: secondSchemaDir,
      server: { port: 19889, adminSecret: "stop-failure-admin-2" },
    });
    expect(process.env.BACKEND_SECRET).toBe("stop-failure-backend-2");
    await runtime.dispose();
  });

  it("separates a generated app ID from a final unterminated env line", async () => {
    const schemaDir = await tempRoots.create("jazz-managed-unterminated-env-");
    const appId = "00000000-0000-0000-0000-000000000125";
    await writeFile(join(schemaDir, ".env"), "EXISTING=value");

    const runtime = makeRuntime();
    expect(runtime.prepareEnv({ appId, schemaDir })).toBe(appId);

    await expect(readFile(join(schemaDir, ".env"), "utf8")).resolves.toBe(
      `EXISTING=value\nVITE_JAZZ_APP_ID=${appId}\n`,
    );
  });

  it("does not mistake comments or longer variable names for the app ID", async () => {
    const schemaDir = await tempRoots.create("jazz-managed-similar-env-");
    const appId = "00000000-0000-0000-0000-000000000126";
    await writeFile(
      join(schemaDir, ".env"),
      "# VITE_JAZZ_APP_ID=disabled\nMY_VITE_JAZZ_APP_ID=other\n",
    );

    const runtime = makeRuntime();
    expect(runtime.prepareEnv({ appId, schemaDir })).toBe(appId);

    await expect(readFile(join(schemaDir, ".env"), "utf8")).resolves.toBe(
      `# VITE_JAZZ_APP_ID=disabled\nMY_VITE_JAZZ_APP_ID=other\nVITE_JAZZ_APP_ID=${appId}\n`,
    );
  });

  it("uses dotenv quoting, comments, whitespace, and export syntax", async () => {
    const schemaDir = await tempRoots.create("jazz-managed-dotenv-syntax-");
    await writeFile(
      join(schemaDir, ".env"),
      'OTHER=value\n  export VITE_JAZZ_APP_ID = "quoted app id" # comment\n',
    );

    expect(makeRuntime().prepareEnv({ schemaDir })).toBe("quoted app id");
  });

  it("rejects duplicate and empty exact assignments without exposing values", async () => {
    const duplicateDir = await tempRoots.create("jazz-managed-dotenv-duplicate-");
    await writeFile(
      join(duplicateDir, ".env"),
      "VITE_JAZZ_APP_ID=first-secret\nexport VITE_JAZZ_APP_ID=second-secret\n",
    );
    expect(() => makeRuntime().prepareEnv({ schemaDir: duplicateDir })).toThrow(
      "VITE_JAZZ_APP_ID is assigned more than once in .env (lines 1, 2)",
    );
    try {
      makeRuntime().prepareEnv({ schemaDir: duplicateDir });
    } catch (error) {
      expect(String(error)).not.toContain("first-secret");
      expect(String(error)).not.toContain("second-secret");
    }

    const emptyDir = await tempRoots.create("jazz-managed-dotenv-empty-");
    await writeFile(join(emptyDir, ".env"), "VITE_JAZZ_APP_ID= # intentionally empty\n");
    expect(() => makeRuntime().prepareEnv({ schemaDir: emptyDir })).toThrow(
      "VITE_JAZZ_APP_ID is empty in .env",
    );
  });

  it("preserves CRLF, inode, and file mode when appending", async () => {
    const schemaDir = await tempRoots.create("jazz-managed-dotenv-preserve-");
    const envPath = join(schemaDir, ".env");
    await writeFile(envPath, "FIRST=one\r\nSECOND=two");
    await chmod(envPath, 0o640);
    const appId = "00000000-0000-0000-0000-000000000127";

    const before = await stat(envPath);
    expect(makeRuntime().prepareEnv({ appId, schemaDir })).toBe(appId);
    await expect(readFile(envPath, "utf8")).resolves.toBe(
      `FIRST=one\r\nSECOND=two\r\nVITE_JAZZ_APP_ID=${appId}\r\n`,
    );
    expect((await stat(envPath)).mode & 0o777).toBe(0o640);
    expect((await stat(envPath)).ino).toBe(before.ino);
  });

  it("creates .env with ordinary umask-derived permissions", async () => {
    const schemaDir = await tempRoots.create("jazz-managed-dotenv-mode-");
    const previousUmask = process.umask(0o027);
    try {
      makeRuntime().prepareEnv({ schemaDir, appId: "mode-id" });
    } finally {
      process.umask(previousUmask);
    }
    expect((await stat(join(schemaDir, ".env"))).mode & 0o777).toBe(0o640);
  });

  it("rejects a symlinked .env without changing its target", async () => {
    const schemaDir = await tempRoots.create("jazz-managed-dotenv-symlink-");
    const target = join(schemaDir, "target.env");
    const envPath = join(schemaDir, ".env");
    await writeFile(target, "EXISTING=untouched\n");
    await symlink(target, envPath);

    expect(() => makeRuntime().prepareEnv({ schemaDir, appId: "not-written" })).toThrow(
      "refusing to update symlinked .env",
    );
    await expect(readFile(target, "utf8")).resolves.toBe("EXISTING=untouched\n");
    expect((await lstat(envPath)).isSymbolicLink()).toBe(true);
  });

  it("serializes concurrent process writers without lost updates", async () => {
    const schemaDir = await tempRoots.create("jazz-managed-dotenv-process-race-");
    const bundlePath = join(schemaDir, "managed-runtime.mjs");
    await build({
      entryPoints: [join(import.meta.dirname, "managed-runtime.ts")],
      outfile: bundlePath,
      bundle: true,
      format: "esm",
      platform: "node",
      external: ["fs-native-extensions"],
    });
    const envPath = join(schemaDir, ".env");
    await writeFile(envPath, "EXISTING=preserved");
    const prefixes = ["VITE", "NEXT_PUBLIC", "EXPO_PUBLIC"];
    const candidates = Array.from({ length: 9 }, (_, index) => ({
      key: `${prefixes[index % prefixes.length]}_JAZZ_APP_ID`,
      value: `00000000-0000-0000-0000-${String(index + 1).padStart(12, "0")}`,
    }));
    const workers = candidates.map(({ key, value }) =>
      execFile(process.execPath, [
        "--input-type=module",
        "--eval",
        `import { ensureEnvAppId } from ${JSON.stringify(bundlePath)}; process.stdout.write(ensureEnvAppId(${JSON.stringify(envPath)}, ${JSON.stringify(key)}, ${JSON.stringify(value)}, undefined));`,
      ]),
    );
    const results = (await Promise.all(workers)).map(({ stdout }) => stdout);
    const content = await readFile(envPath, "utf8");
    expect(content).toContain("EXISTING=preserved\n");
    for (const prefix of prefixes) {
      const key = `${prefix}_JAZZ_APP_ID`;
      const matchingResults = results.filter((_, index) => candidates[index]?.key === key);
      expect(new Set(matchingResults).size).toBe(1);
      expect(content.match(new RegExp(`^${key}=`, "gm"))).toHaveLength(1);
      expect(content).toContain(`${key}=${matchingResults[0]}\n`);
    }
  });

  it("recovers the advisory lock after a writer is killed", async () => {
    if (process.platform === "win32") return;
    const schemaDir = await tempRoots.create("jazz-managed-dotenv-crash-");
    const envPath = join(schemaDir, ".env");
    await writeFile(envPath, "EXISTING=preserved\n");
    const worker = spawn(
      process.execPath,
      [
        "--input-type=module",
        "--eval",
        `import { openSync } from "node:fs"; import { waitForLockSync } from "fs-native-extensions"; const fd=openSync(${JSON.stringify(envPath)}, "a+"); waitForLockSync(fd); process.stdout.write("locked\\n"); setInterval(()=>{}, 1000);`,
      ],
      { stdio: ["ignore", "pipe", "inherit"] },
    );
    await new Promise<void>((resolve, reject) => {
      worker.once("error", reject);
      worker.stdout.once("data", () => resolve());
    });
    worker.kill("SIGKILL");
    await new Promise<void>((resolve) => worker.once("exit", () => resolve()));

    expect(makeRuntime().prepareEnv({ schemaDir, appId: "after-crash" })).toBe("after-crash");
    await expect(readFile(envPath, "utf8")).resolves.toContain("VITE_JAZZ_APP_ID=after-crash\n");
  });

  it("waits for a live lock owner instead of stealing its lock", async () => {
    const schemaDir = await tempRoots.create("jazz-managed-dotenv-live-owner-");
    const envPath = join(schemaDir, ".env");
    const bundlePath = join(schemaDir, "managed-runtime.mjs");
    await writeFile(envPath, "EXISTING=preserved\n");
    await build({
      entryPoints: [join(import.meta.dirname, "managed-runtime.ts")],
      outfile: bundlePath,
      bundle: true,
      format: "esm",
      platform: "node",
      external: ["fs-native-extensions"],
    });
    const descriptor = openSync(envPath, "a+");
    waitForLockSync(descriptor);
    const worker = execFile(process.execPath, [
      "--input-type=module",
      "--eval",
      `import { ensureEnvAppId } from ${JSON.stringify(bundlePath)}; process.stdout.write(ensureEnvAppId(${JSON.stringify(envPath)}, "VITE_JAZZ_APP_ID", "after-owner", undefined));`,
    ]);
    let settled = false;
    void worker.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );
    try {
      await new Promise((resolve) => setTimeout(resolve, 75));
      expect(settled).toBe(false);
    } finally {
      unlock(descriptor);
      closeSync(descriptor);
    }
    await expect(worker).resolves.toMatchObject({ stdout: "after-owner" });
  });

  it("preserves external edits made before the locked append", async () => {
    const schemaDir = await tempRoots.create("jazz-managed-dotenv-external-");
    const envPath = join(schemaDir, ".env");
    await writeFile(envPath, "EXTERNAL=first\n");
    await writeFile(envPath, "EXTERNAL=changed\nOTHER=kept\n");
    ensureEnvAppId(envPath, "VITE_JAZZ_APP_ID", "generated", undefined);
    await expect(readFile(envPath, "utf8")).resolves.toBe(
      "EXTERNAL=changed\nOTHER=kept\nVITE_JAZZ_APP_ID=generated\n",
    );
  });

  it("reparses an unrelated external append after acquiring the lock", async () => {
    const schemaDir = await tempRoots.create("jazz-managed-dotenv-external-locked-");
    const envPath = join(schemaDir, ".env");
    await writeFile(envPath, "FIRST=one\n");
    ensureEnvAppId(envPath, "VITE_JAZZ_APP_ID", "generated", undefined, {
      waitForLock(descriptor) {
        waitForLockSync(descriptor);
        // Model a non-cooperating editor that appends after the Jazz process has
        // opened the file but before it reparses the latest bytes.
        appendFileSync(envPath, "EXTERNAL=kept\n");
      },
      unlock,
      write: (descriptor, bytes, offset) =>
        writeSync(descriptor, bytes, offset, bytes.byteLength - offset),
      fsync: fsyncSync,
    });
    await expect(readFile(envPath, "utf8")).resolves.toBe(
      "FIRST=one\nEXTERNAL=kept\nVITE_JAZZ_APP_ID=generated\n",
    );
  });

  it("retries against an atomic replacement made after lock acquisition", async () => {
    const schemaDir = await tempRoots.create("jazz-managed-dotenv-replaced-locked-");
    const envPath = join(schemaDir, ".env");
    const replacementPath = join(schemaDir, ".env.replacement");
    await writeFile(envPath, "ORIGINAL=old\n");
    let replaced = false;

    expect(
      ensureEnvAppId(envPath, "VITE_JAZZ_APP_ID", "generated", undefined, {
        waitForLock(descriptor) {
          waitForLockSync(descriptor);
          if (!replaced) {
            writeFileSync(replacementPath, "REPLACED=visible\n");
            renameSync(replacementPath, envPath);
            replaced = true;
          }
        },
        unlock,
        write: (descriptor, bytes, offset) =>
          writeSync(descriptor, bytes, offset, bytes.byteLength - offset),
        fsync: fsyncSync,
      }),
    ).toBe("generated");

    await expect(readFile(envPath, "utf8")).resolves.toBe(
      "REPLACED=visible\nVITE_JAZZ_APP_ID=generated\n",
    );
  });

  it("retries an atomic replacement between opening .env and its first identity check", async () => {
    const schemaDir = await tempRoots.create("jazz-managed-dotenv-replaced-opening-");
    const envPath = join(schemaDir, ".env");
    const replacementPath = join(schemaDir, ".env.replacement");
    await writeFile(envPath, "ORIGINAL=old\n");
    let replaced = false;

    expect(
      ensureEnvAppId(envPath, "VITE_JAZZ_APP_ID", "generated", undefined, {
        afterOpen() {
          if (!replaced) {
            writeFileSync(replacementPath, "REPLACED=visible\n");
            renameSync(replacementPath, envPath);
            replaced = true;
          }
        },
        waitForLock: waitForLockSync,
        unlock,
        write: (descriptor, bytes, offset) =>
          writeSync(descriptor, bytes, offset, bytes.byteLength - offset),
        fsync: fsyncSync,
      }),
    ).toBe("generated");

    await expect(readFile(envPath, "utf8")).resolves.toBe(
      "REPLACED=visible\nVITE_JAZZ_APP_ID=generated\n",
    );
  });

  it("retries when .env is atomically replaced after the append", async () => {
    const schemaDir = await tempRoots.create("jazz-managed-dotenv-replaced-final-");
    const envPath = join(schemaDir, ".env");
    const replacementPath = join(schemaDir, ".env.replacement");
    await writeFile(envPath, "ORIGINAL=old\n");
    let replaced = false;

    expect(
      ensureEnvAppId(envPath, "VITE_JAZZ_APP_ID", "generated", undefined, {
        waitForLock: waitForLockSync,
        unlock,
        write: (descriptor, bytes, offset) =>
          writeSync(descriptor, bytes, offset, bytes.byteLength - offset),
        fsync(descriptor) {
          fsyncSync(descriptor);
          if (!replaced) {
            writeFileSync(replacementPath, "REPLACED=visible\n");
            renameSync(replacementPath, envPath);
            replaced = true;
          }
        },
      }),
    ).toBe("generated");

    await expect(readFile(envPath, "utf8")).resolves.toBe(
      "REPLACED=visible\nVITE_JAZZ_APP_ID=generated\n",
    );
  });

  it("full-writes short writes and surfaces write, fsync, and unlock errors", async () => {
    const makeOperations = () => ({
      waitForLock: waitForLockSync,
      unlock,
      write: (descriptor: number, bytes: Uint8Array, offset: number) =>
        writeSync(descriptor, bytes, offset, Math.min(2, bytes.byteLength - offset)),
      fsync: fsyncSync,
    });
    const shortDir = await tempRoots.create("jazz-managed-dotenv-short-write-");
    ensureEnvAppId(
      join(shortDir, ".env"),
      "VITE_JAZZ_APP_ID",
      "short",
      undefined,
      makeOperations(),
    );
    await expect(readFile(join(shortDir, ".env"), "utf8")).resolves.toBe(
      "VITE_JAZZ_APP_ID=short\n",
    );

    for (const failure of ["write", "fsync", "unlock"] as const) {
      const schemaDir = await tempRoots.create(`jazz-managed-dotenv-${failure}-`);
      const operations = makeOperations();
      operations[failure] = (() => {
        throw new Error(`${failure}-failed`);
      }) as never;
      expect(() =>
        ensureEnvAppId(join(schemaDir, ".env"), "VITE_JAZZ_APP_ID", failure, undefined, operations),
      ).toThrow(`${failure}-failed`);
      // close(2) releases the OS lock even if explicit unlock reports an error.
      expect(() =>
        ensureEnvAppId(join(schemaDir, ".env"), "OTHER", "retry", undefined),
      ).not.toThrow();
    }
  });

  it("does not print the local server banner when stdout is not interactive", async () => {
    const schemaDir = await tempRoots.create("jazz-managed-noninteractive-banner-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());
    setStdoutIsTTY(false);

    const log = vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000123",
      port: 19883,
      url: "http://127.0.0.1:19883",
      dataDir: join(schemaDir, "node_modules", ".cache", "jazz-dev-server"),
      adminSecret: "noninteractive-admin",
      backendSecret: "noninteractive-backend",
      stop: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(catalogueProject, "deploy").mockResolvedValue(deployed());
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const runtime = makeRuntime();
    try {
      await runtime.initialize({
        schemaDir,
        server: { port: 19883, adminSecret: "noninteractive-admin" },
      });

      expect(log).not.toHaveBeenCalledWith(expect.stringContaining("Running a local jazz server"));
      expect(log).toHaveBeenCalledWith("[jazz] schema published");
    } finally {
      await runtime.dispose();
    }
  });

  it("keeps env-driven Cloud startup alive when the initial schema push cannot reach the server", async () => {
    const schemaDir = await tempRoots.create("jazz-managed-offline-cloud-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());

    process.env.VITE_JAZZ_APP_ID = "00000000-0000-0000-0000-000000000777";
    process.env.VITE_JAZZ_SERVER_URL = "https://v2.sync.jazz.tools/";
    process.env.JAZZ_ADMIN_SECRET = "cloud-admin-secret";

    const startLocalJazzServer = vi.spyOn(devServer, "startLocalJazzServer");
    vi.spyOn(catalogueProject, "deploy").mockRejectedValue(makeFetchFailedError("ENOTFOUND"));
    const watchSchema = vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({
      close: vi.fn(),
    });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

    const runtime = makeRuntime();
    const managed = await runtime.initialize({ schemaDir });

    expect(managed).toMatchObject({
      appId: "00000000-0000-0000-0000-000000000777",
      serverUrl: "https://v2.sync.jazz.tools/",
      adminSecret: "cloud-admin-secret",
    });
    expect(startLocalJazzServer).not.toHaveBeenCalled();
    expect(watchSchema).toHaveBeenCalledWith(
      expect.objectContaining({
        appId: "00000000-0000-0000-0000-000000000777",
        serverUrl: "https://v2.sync.jazz.tools/",
        adminSecret: "cloud-admin-secret",
        schemaDir,
      }),
    );
    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining(
        "schema auto-push skipped because https://v2.sync.jazz.tools/ is unreachable",
      ),
    );
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("comment out VITE_JAZZ_SERVER_URL"));

    await runtime.dispose();
  });

  it("still fails env-driven startup when the initial schema push reaches the server and is rejected", async () => {
    const schemaDir = await tempRoots.create("jazz-managed-cloud-rejected-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());

    process.env.VITE_JAZZ_APP_ID = "00000000-0000-0000-0000-000000000888";
    process.env.VITE_JAZZ_SERVER_URL = "https://v2.sync.jazz.tools/";
    process.env.JAZZ_ADMIN_SECRET = "cloud-admin-secret";

    vi.spyOn(catalogueProject, "deploy").mockRejectedValue(
      new Error("Schema publish failed: 401 Unauthorized"),
    );
    vi.spyOn(console, "error").mockImplementation(() => {});

    await expect(makeRuntime().initialize({ schemaDir })).rejects.toThrow(
      "Schema publish failed: 401 Unauthorized",
    );
  });

  it("does not skip non-fetch errors just because their message contains a network error code", async () => {
    const schemaDir = await tempRoots.create("jazz-managed-cloud-non-fetch-error-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());

    process.env.VITE_JAZZ_APP_ID = "00000000-0000-0000-0000-000000000999";
    process.env.VITE_JAZZ_SERVER_URL = "https://v2.sync.jazz.tools/";
    process.env.JAZZ_ADMIN_SECRET = "cloud-admin-secret";

    vi.spyOn(catalogueProject, "deploy").mockRejectedValue(
      new Error("getaddrinfo ENOTFOUND v2.sync.jazz.tools"),
    );
    vi.spyOn(console, "error").mockImplementation(() => {});

    await expect(makeRuntime().initialize({ schemaDir })).rejects.toThrow(
      "getaddrinfo ENOTFOUND v2.sync.jazz.tools",
    );
  });
});
