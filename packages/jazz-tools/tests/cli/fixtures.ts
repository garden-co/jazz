import { spawn, spawnSync, type SpawnSyncReturns } from "node:child_process";
import { createServer, type Server } from "node:http";
import { mkdtempSync } from "node:fs";
import { access, mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { afterAll, afterEach, vi } from "vitest";

export function createCliFixtures(moduleUrl: string) {
  const distCliPath = fileURLToPath(new URL("../dist/cli.js", moduleUrl));
  const binPath = fileURLToPath(new URL("../bin/jazz-tools.js", moduleUrl));

  const tmpBase = mkdtempSync(join(tmpdir(), "jazz-tools-cli-tests-"));
  afterAll(() => rm(tmpBase, { recursive: true, force: true }));
  const tempRoots: string[] = [];
  const APP_ID = "test-app";

  afterEach(async () => {
    vi.unstubAllGlobals();
    await Promise.all(
      tempRoots.splice(0).map((root) => rm(root, { recursive: true, force: true })),
    );
  });

  async function createWorkspace(): Promise<{ root: string; schemaDir: string }> {
    await mkdir(tmpBase, { recursive: true });
    const root = await mkdtemp(join(tmpBase, "jazz-tools-cli-test-"));
    tempRoots.push(root);
    const schemaDir = join(root, "schema");
    await mkdir(schemaDir, { recursive: true });
    await writeFile(join(root, "package.json"), '{ "type": "module" }\n');
    return { root, schemaDir };
  }

  async function fileExists(path: string): Promise<boolean> {
    try {
      await access(path);
      return true;
    } catch {
      return false;
    }
  }

  async function waitForCrashMarker(
    marker: string,
    child: ReturnType<typeof spawn>,
  ): Promise<void> {
    const deadline = Date.now() + 5_000;
    while (!(await fileExists(marker))) {
      if (child.exitCode !== null) {
        throw new Error(`migration child exited before reaching ${marker}`);
      }
      if (Date.now() >= deadline) throw new Error(`migration child did not reach ${marker}`);
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }

  async function waitForMarkers(markers: readonly string[]): Promise<void> {
    const deadline = Date.now() + 5_000;
    while (!(await Promise.all(markers.map(fileExists))).every(Boolean)) {
      if (Date.now() >= deadline) {
        throw new Error(
          `migration children did not all reach lock contention: ${markers.join(", ")}`,
        );
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }

  async function killChild(child: ReturnType<typeof spawn>): Promise<void> {
    if (child.exitCode !== null) return;
    const closed = new Promise<void>((resolve) => child.once("close", () => resolve()));
    child.kill("SIGKILL");
    await closed;
  }

  async function captureConsoleLogs<T>(
    run: () => Promise<T>,
  ): Promise<{ result: T; logs: string[] }> {
    const logs: string[] = [];
    const ansiEscape = String.fromCodePoint(27);
    const ansiSgrPattern = new RegExp(`${ansiEscape}\\[[0-9;]*m`, "g");
    const stripAnsi = (line: string): string => line.replace(ansiSgrPattern, "");
    const logSpy = vi
      .spyOn(console, "log")
      .mockImplementation((message?: unknown, ...rest: unknown[]) => {
        logs.push(stripAnsi([message, ...rest].map((value) => String(value ?? "")).join(" ")));
      });
    const warnSpy = vi
      .spyOn(console, "warn")
      .mockImplementation((message?: unknown, ...rest: unknown[]) => {
        logs.push(stripAnsi([message, ...rest].map((value) => String(value ?? "")).join(" ")));
      });

    try {
      const result = await run();
      return { result, logs };
    } finally {
      warnSpy.mockRestore();
      logSpy.mockRestore();
    }
  }

  /** Write a project's `jazz.app.json` and, unless `null`, its `jazz.permissions.json`. */
  async function writeAppFiles(
    dir: string,
    app: object = todosApp(),
    permissions: object | null = todosPermissions(),
  ): Promise<void> {
    await mkdir(dir, { recursive: true });
    await writeFile(join(dir, "jazz.app.json"), `${JSON.stringify(app, null, 2)}\n`);
    if (permissions) {
      await writeFile(
        join(dir, "jazz.permissions.json"),
        `${JSON.stringify(permissions, null, 2)}\n`,
      );
    }
  }

  /** `projects` and `todos`, the app most CLI tests run on. */
  function todosApp() {
    return {
      format: 1,
      tables: {
        projects: { columns: { name: "string" } },
        todos: { columns: { title: "string", ownerId: "string" } },
      },
    };
  }

  /** `todosApp` with an optional `notes` column on todos. */
  function todosAppWithNotes() {
    return {
      format: 1,
      tables: {
        projects: { columns: { name: "string" } },
        todos: { columns: { title: "string", ownerId: "string", notes: "string?" } },
      },
    };
  }

  /** Owners read their todos; nothing else is allowed. */
  function todosPermissions() {
    return {
      format: 1,
      tables: { todos: { read: "ownerId = session.identity.subject" } },
    };
  }

  /** A permissions file that allows nothing. */
  function emptyPermissions() {
    return { format: 1, tables: {} };
  }

  function runBin(
    args: string[],
    options: { cwd?: string; env?: NodeJS.ProcessEnv } = {},
  ): SpawnSyncReturns<string> {
    return spawnSync(process.execPath, ["--no-warnings", binPath, ...args], {
      encoding: "utf8",
      cwd: options.cwd,
      env: options.env ?? process.env,
    });
  }

  async function runCli(
    args: readonly string[],
    options: { cwd?: string; env?: NodeJS.ProcessEnv } = {},
  ): Promise<{ status: number | null; stdout: string; stderr: string }> {
    let resolve!: (value: { status: number | null; stdout: string; stderr: string }) => void;
    const promise = new Promise<{ status: number | null; stdout: string; stderr: string }>(
      (resolvePromise) => {
        resolve = resolvePromise;
      },
    );
    const child = spawn(process.execPath, ["--no-warnings", distCliPath, ...args], {
      cwd: options.cwd,
      env: options.env ?? process.env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk: Buffer) => {
      stdout += chunk.toString();
    });
    child.stderr.on("data", (chunk: Buffer) => {
      stderr += chunk.toString();
    });
    child.on("close", (status) => resolve({ status, stdout, stderr }));
    return promise;
  }

  async function listenForDeployRequest(): Promise<{ server: Server; url: string }> {
    const server = createServer((request, response) => {
      response.statusCode = 400;
      response.end(
        `request=${request.url} secret=${request.headers["x-jazz-admin-secret"] ?? "<missing>"}`,
      );
    });
    let resolveListening!: () => void;
    let rejectListening!: (reason?: unknown) => void;
    const listening = new Promise<void>((resolvePromise, rejectPromise) => {
      resolveListening = resolvePromise;
      rejectListening = rejectPromise;
    });
    server.once("error", rejectListening);
    server.listen(0, "127.0.0.1", resolveListening);
    await listening;
    const address = server.address();
    if (!address || typeof address === "string") {
      throw new Error("Expected deploy test server to have a TCP address.");
    }
    return { server, url: `http://127.0.0.1:${address.port}` };
  }

  return {
    distCliPath,
    binPath,
    tmpBase,
    tempRoots,
    APP_ID,
    createWorkspace,
    fileExists,
    waitForCrashMarker,
    waitForMarkers,
    killChild,
    captureConsoleLogs,
    writeAppFiles,
    todosApp,
    todosAppWithNotes,
    todosPermissions,
    emptyPermissions,
    runBin,
    runCli,
    listenForDeployRequest,
  };
}
