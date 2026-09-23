import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { createTempRootTracker, getAvailablePort, todoSchema } from "./test-helpers.js";
import * as devServer from "./dev-server.js";
import * as catalogueProject from "./catalogue-project.js";
import * as schemaWatcher from "./schema-watcher.js";
import { jazzSvelteKit, __resetJazzSvelteKitPluginForTests } from "./sveltekit.js";
import type { ViteDevServer } from "./vite.js";

const dev = await import("./index.js");

const tempRoots = createTempRootTracker();
const originalJazzAppId = process.env.PUBLIC_JAZZ_APP_ID;
const originalJazzServerUrl = process.env.PUBLIC_JAZZ_SERVER_URL;
const originalJazzTelemetryCollectorUrl = process.env.PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL;
const originalBackendSecret = process.env.BACKEND_SECRET;

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

type CapturedMiddleware = Parameters<NonNullable<ViteDevServer["middlewares"]>["use"]>[0];

function invokeMiddleware(handler: CapturedMiddleware, url: string) {
  return new Promise<{ statusCode: number; body: string }>((resolve) => {
    let statusCode = 200;
    handler(
      { url },
      {
        setHeader() {},
        get statusCode() {
          return statusCode;
        },
        set statusCode(value: number) {
          statusCode = value;
        },
        end(body?: string | Buffer) {
          resolve({ statusCode, body: body?.toString() ?? "" });
        },
      },
      () => resolve({ statusCode, body: "" }),
    );
  });
}

function makeViteServer(
  command: "serve" | "build",
  root = "/tmp/jazz-sveltekit-test",
): ViteDevServer & {
  restart: ReturnType<typeof vi.fn>;
  middlewareHandlers: CapturedMiddleware[];
} {
  const middlewareHandlers: CapturedMiddleware[] = [];
  return {
    config: { root, command, env: {} },
    httpServer: {
      once() {},
    },
    middlewares: {
      use(handler) {
        middlewareHandlers.push(handler);
      },
    },
    ws: { send() {} },
    restart: vi.fn(() => Promise.resolve()),
    middlewareHandlers,
  };
}

// Managed-runtime writes PUBLIC_JAZZ_APP_ID / PUBLIC_JAZZ_SERVER_URL to
// process.env on successful init; that state leaks across vitest workers in the
// same thread pool and flips later tests onto the env-driven cloud branch.
// Scrub before each test so every case starts from the same baseline.
beforeEach(() => {
  delete process.env.PUBLIC_JAZZ_APP_ID;
  delete process.env.PUBLIC_JAZZ_SERVER_URL;
  delete process.env.PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL;
  delete process.env.JAZZ_ADMIN_SECRET;
  delete process.env.BACKEND_SECRET;
});

afterEach(async () => {
  await __resetJazzSvelteKitPluginForTests();
  await tempRoots.cleanup();
  // Shared /tmp roots accumulate .env files from managed-runtime's app-id
  // persistence; wipe them so the plugin's env-file backfill starts clean.
  for (const shared of ["/tmp/jazz-sveltekit-test", "/tmp/jazz-sk-noserver"]) {
    await rm(join(shared, ".env"), { force: true }).catch(() => undefined);
  }
  vi.restoreAllMocks();

  if (originalJazzAppId === undefined) {
    delete process.env.PUBLIC_JAZZ_APP_ID;
  } else {
    process.env.PUBLIC_JAZZ_APP_ID = originalJazzAppId;
  }

  if (originalJazzServerUrl === undefined) {
    delete process.env.PUBLIC_JAZZ_SERVER_URL;
  } else {
    process.env.PUBLIC_JAZZ_SERVER_URL = originalJazzServerUrl;
  }

  if (originalJazzTelemetryCollectorUrl === undefined) {
    delete process.env.PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL;
  } else {
    process.env.PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL = originalJazzTelemetryCollectorUrl;
  }

  if (originalBackendSecret === undefined) {
    delete process.env.BACKEND_SECRET;
  } else {
    process.env.BACKEND_SECRET = originalBackendSecret;
  }
});

describe("jazzSvelteKit", () => {
  it("config hook accepts both Vite ssr.external shapes (true and string[])", () => {
    const plugin = jazzSvelteKit();
    // Without a serve ConfigEnv the hook returns the merged config synchronously
    // (the async runtime path only runs for `command: "serve"`).

    const runConfig = (c: Record<string, unknown>) =>
      plugin.config(c) as { ssr: { external: true | string[] } };

    const arrayResult = runConfig({ ssr: { external: ["other-pkg"] } });
    expect(arrayResult.ssr.external).toContain("jazz-napi");
    expect(arrayResult.ssr.external).toContain("other-pkg");

    const externaliseAll = runConfig({ ssr: { external: true } });
    expect(externaliseAll.ssr.external).toBe(true);
  });
  it("serves the embedded inspector route and disposes the runtime on plugin close", async () => {
    const stop = vi.fn().mockResolvedValue(undefined);
    vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000237",
      port: 19987,
      url: "http://127.0.0.1:19987",
      dataDir: undefined as unknown as string,
      adminSecret: "local-admin",
      backendSecret: "local-backend",
      stop,
    });
    vi.spyOn(catalogueProject, "deploy").mockResolvedValue(deployed("bug237"));
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const root = await tempRoots.create("jazz-sveltekit-inspector-test-");
    const plugin = jazzSvelteKit({
      server: { port: 19987, adminSecret: "bug237-admin" },
    });
    const viteServer = makeViteServer("serve", root);
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(viteServer);

    expect(viteServer.config.env!.VITE_JAZZ_INSPECTOR).toBe("1");
    expect(viteServer.middlewareHandlers).toHaveLength(1);
    const response = await invokeMiddleware(
      viteServer.middlewareHandlers[0]!,
      "/__jazz/embedded/embedded.html",
    );
    expect(response.statusCode).toBe(200);
    expect(response.body).toContain("<!doctype html>");

    await (plugin.closeBundle as () => Promise<void>)();
    expect(stop).toHaveBeenCalledOnce();
  });

  it("restarts the same plugin after close with a top-level adminSecret", async () => {
    const firstStop = vi.fn().mockResolvedValue(undefined);
    const secondStop = vi.fn().mockResolvedValue(undefined);
    vi.spyOn(devServer, "startLocalJazzServer")
      .mockResolvedValueOnce({
        appId: "00000000-0000-0000-0000-000000000239",
        port: 19985,
        url: "http://127.0.0.1:19985",
        dataDir: undefined as unknown as string,
        adminSecret: "local-admin",
        backendSecret: "local-backend",
        stop: firstStop,
      })
      .mockResolvedValueOnce({
        appId: "00000000-0000-0000-0000-000000000240",
        port: 19984,
        url: "http://127.0.0.1:19984",
        dataDir: undefined as unknown as string,
        adminSecret: "local-admin",
        backendSecret: "local-backend",
        stop: secondStop,
      });
    vi.spyOn(catalogueProject, "deploy").mockResolvedValue(deployed("bug237-restart"));
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const root = await tempRoots.create("jazz-sveltekit-restart-test-");
    const plugin = jazzSvelteKit({ adminSecret: "top-level-admin" });
    const configureServer = plugin.configureServer as (s: ViteDevServer) => Promise<void>;
    await configureServer(makeViteServer("serve", root));
    await (plugin.closeBundle as () => Promise<void>)();
    await configureServer(makeViteServer("serve", root));

    expect(devServer.startLocalJazzServer).toHaveBeenCalledTimes(2);
    expect(firstStop).toHaveBeenCalledOnce();
    await (plugin.closeBundle as () => Promise<void>)();
    expect(secondStop).toHaveBeenCalledOnce();
  });

  it("disposes from closeBundle when Vite is running in middleware mode", async () => {
    const stop = vi.fn().mockResolvedValue(undefined);
    vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000241",
      port: 19983,
      url: "http://127.0.0.1:19983",
      dataDir: undefined as unknown as string,
      adminSecret: "local-admin",
      backendSecret: "local-backend",
      stop,
    });
    vi.spyOn(catalogueProject, "deploy").mockResolvedValue(deployed("bug237-middleware"));
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const root = await tempRoots.create("jazz-sveltekit-middleware-test-");
    const plugin = jazzSvelteKit({ server: { adminSecret: "middleware-admin" } });
    const viteServer = { ...makeViteServer("serve", root), httpServer: null };
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(viteServer);

    await (plugin.closeBundle as () => Promise<void>)();
    expect(stop).toHaveBeenCalledOnce();
  });

  it("shares one disposal when closeBundle calls are concurrent", async () => {
    const stop = vi.fn().mockResolvedValue(undefined);
    vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000242",
      port: 19982,
      url: "http://127.0.0.1:19982",
      dataDir: undefined as unknown as string,
      adminSecret: "local-admin",
      backendSecret: "local-backend",
      stop,
    });
    vi.spyOn(catalogueProject, "deploy").mockResolvedValue(deployed("bug237-concurrent"));
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const root = await tempRoots.create("jazz-sveltekit-concurrent-close-test-");
    const plugin = jazzSvelteKit({ server: { adminSecret: "concurrent-admin" } });
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(
      makeViteServer("serve", root),
    );

    await Promise.all([
      (plugin.closeBundle as () => Promise<void>)(),
      (plugin.closeBundle as () => Promise<void>)(),
    ]);

    expect(stop).toHaveBeenCalledOnce();
  });
  it("keeps a new server reference installed when configure races closeBundle disposal", async () => {
    let resolveFirstStop!: () => void;
    const firstStopPending = new Promise<void>((resolve) => {
      resolveFirstStop = resolve;
    });
    const firstStop = vi.fn().mockReturnValue(firstStopPending);
    const secondStop = vi.fn().mockResolvedValue(undefined);
    const startSpy = vi.spyOn(devServer, "startLocalJazzServer");
    startSpy
      .mockResolvedValueOnce({
        appId: "00000000-0000-0000-0000-000000000244",
        port: 19980,
        url: "http://127.0.0.1:19980",
        dataDir: undefined as unknown as string,
        adminSecret: "local-admin",
        backendSecret: "local-backend",
        stop: firstStop,
      })
      .mockResolvedValueOnce({
        appId: "00000000-0000-0000-0000-000000000245",
        port: 19979,
        url: "http://127.0.0.1:19979",
        dataDir: undefined as unknown as string,
        adminSecret: "local-admin",
        backendSecret: "local-backend",
        stop: secondStop,
      });
    vi.spyOn(catalogueProject, "deploy").mockResolvedValue(deployed("bug237-race"));
    let latestOnPush: ((hash: string) => void | Promise<void>) | undefined;
    vi.spyOn(schemaWatcher, "watchSchema").mockImplementation((opts) => {
      latestOnPush = opts.onPush;
      return { close: vi.fn() };
    });

    const root = await tempRoots.create("jazz-sveltekit-race-test-");
    const plugin = jazzSvelteKit({ server: { adminSecret: "race-admin" } });
    const configureServer = plugin.configureServer as (s: ViteDevServer) => Promise<void>;
    const firstServer = makeViteServer("serve", root);
    await configureServer(firstServer);

    const closePromise = (plugin.closeBundle as () => Promise<void>)();
    expect(firstStop).toHaveBeenCalledOnce();

    const newWsSend = vi.fn();
    const newServer = {
      ...makeViteServer("serve", root),
      ws: { send: newWsSend },
    };
    const configurePromise = configureServer(newServer);
    await Promise.resolve();
    expect(startSpy).toHaveBeenCalledOnce();

    resolveFirstStop();
    await closePromise;
    await configurePromise;
    expect(startSpy).toHaveBeenCalledTimes(2);

    newWsSend.mockClear();
    expect(latestOnPush).toBeDefined();
    await latestOnPush!("b237aaceface");
    expect(newWsSend).toHaveBeenCalledWith({ type: "full-reload" });

    await (plugin.closeBundle as () => Promise<void>)();
  });

  it("keeps the runtime alive when Vite restarts and closes the old plugin instance last", async () => {
    const stop = vi.fn().mockResolvedValue(undefined);
    const watcherClose = vi.fn();
    const startSpy = vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000246",
      port: 19978,
      url: "http://127.0.0.1:19978",
      dataDir: undefined as unknown as string,
      adminSecret: "local-admin",
      backendSecret: "local-backend",
      stop,
    });
    vi.spyOn(catalogueProject, "deploy").mockResolvedValue(deployed("bug237-restart-order"));
    let onPush: ((hash: string) => void | Promise<void>) | undefined;
    vi.spyOn(schemaWatcher, "watchSchema").mockImplementation((opts) => {
      onPush = opts.onPush;
      return { close: watcherClose };
    });

    const root = await tempRoots.create("jazz-sveltekit-restart-order-test-");
    const pluginOptions = { server: { port: 19978, adminSecret: "restart-order-admin" } };
    const serveEnv = { command: "serve" as const, mode: "development" };

    // Initial start: Vite runs `config`, then `configureServer`.
    const oldPlugin = jazzSvelteKit(pluginOptions);
    await oldPlugin.config({ root }, serveEnv);
    const oldWsSend = vi.fn();
    await (oldPlugin.configureServer as (s: ViteDevServer) => Promise<void>)({
      ...makeViteServer("serve", root),
      ws: { send: oldWsSend },
    });
    expect(process.env.PUBLIC_JAZZ_SERVER_URL).toBe("http://127.0.0.1:19978");
    expect(process.env.BACKEND_SECRET).toBe("local-backend");
    const appId = process.env.PUBLIC_JAZZ_APP_ID;
    expect(appId).toBeTruthy();

    // Restart (.env or vite.config.ts change): Vite's restartServer builds the
    // new server first — fresh plugin instances run `config` and
    // `configureServer` — and only then closes the old server, which fires the
    // old instance's `closeBundle`.
    const newPlugin = jazzSvelteKit(pluginOptions);
    await newPlugin.config({ root }, serveEnv);
    const newWsSend = vi.fn();
    const newServer = { ...makeViteServer("serve", root), ws: { send: newWsSend } };
    await (newPlugin.configureServer as (s: ViteDevServer) => Promise<void>)(newServer);
    await (oldPlugin.closeBundle as () => Promise<void>)();

    expect(startSpy).toHaveBeenCalledOnce();
    expect(stop).not.toHaveBeenCalled();
    expect(watcherClose).not.toHaveBeenCalled();
    expect(process.env.PUBLIC_JAZZ_APP_ID).toBe(appId);
    expect(process.env.PUBLIC_JAZZ_SERVER_URL).toBe("http://127.0.0.1:19978");
    expect(process.env.BACKEND_SECRET).toBe("local-backend");
    expect(newServer.config.env!.PUBLIC_JAZZ_SERVER_URL).toBe("http://127.0.0.1:19978");

    // Schema pushes after the restart reload the browser on the live server.
    oldWsSend.mockClear();
    await onPush!("b237aaceface01");
    expect(newWsSend).toHaveBeenCalledWith({ type: "full-reload" });
    expect(oldWsSend).not.toHaveBeenCalled();

    // Final shutdown: closing the last active instance disposes everything.
    await (newPlugin.closeBundle as () => Promise<void>)();
    expect(stop).toHaveBeenCalledOnce();
    expect(watcherClose).toHaveBeenCalledOnce();
    expect(process.env.PUBLIC_JAZZ_APP_ID).toBeUndefined();
    expect(process.env.PUBLIC_JAZZ_SERVER_URL).toBeUndefined();
    expect(process.env.BACKEND_SECRET).toBeUndefined();
  });

  it("reports and rethrows disposal failures from closeBundle", async () => {
    const disposalError = new Error("dispose failed");
    const stop = vi.fn().mockRejectedValue(disposalError);
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000243",
      port: 19981,
      url: "http://127.0.0.1:19981",
      dataDir: undefined as unknown as string,
      adminSecret: "local-admin",
      backendSecret: "local-backend",
      stop,
    });
    vi.spyOn(catalogueProject, "deploy").mockResolvedValue(deployed("bug237-reject"));
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const root = await tempRoots.create("jazz-sveltekit-disposal-rejection-test-");
    const plugin = jazzSvelteKit({ server: { adminSecret: "rejection-admin" } });
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(
      makeViteServer("serve", root),
    );

    await expect((plugin.closeBundle as () => Promise<void>)()).rejects.toThrow("dispose failed");
    expect(errorSpy).toHaveBeenCalledWith(
      expect.stringContaining("[jazz] runtime disposal failed"),
      disposalError,
    );
  });

  it("starts a local server in dev and injects PUBLIC_JAZZ_* env vars", async () => {
    const port = await getAvailablePort();
    const root = await tempRoots.create("jazz-sveltekit-test-");
    await mkdir(join(root, "src", "lib"), { recursive: true });
    await writeFile(join(root, "src", "lib", "schema.ts"), todoSchema());
    await writeFile(join(root, "src", "lib", "permissions.ts"), "export default {};\n");

    const plugin = jazzSvelteKit({
      server: { port, adminSecret: "sveltekit-test-admin" },
    });
    const viteServer = makeViteServer("serve", root);
    const configureServer = plugin.configureServer as (server: typeof viteServer) => Promise<void>;
    await configureServer(viteServer);

    const healthResponse = await fetch(`http://127.0.0.1:${port}/health`);
    expect(healthResponse.ok).toBe(true);

    const schemasResponse = await fetch(
      `http://127.0.0.1:${port}/apps/${viteServer.config.env!.PUBLIC_JAZZ_APP_ID}/schemas`,
      {
        headers: { "X-Jazz-Admin-Secret": "sveltekit-test-admin" },
      },
    );
    expect(schemasResponse.ok).toBe(true);
    const body = (await schemasResponse.json()) as { hashes?: string[] };
    expect(body.hashes?.length).toBeGreaterThan(0);

    expect(viteServer.config.env!.PUBLIC_JAZZ_APP_ID).toBeTruthy();
    expect(viteServer.config.env!.PUBLIC_JAZZ_SERVER_URL).toBe(`http://127.0.0.1:${port}`);
    expect(process.env.PUBLIC_JAZZ_APP_ID).toBe(viteServer.config.env!.PUBLIC_JAZZ_APP_ID);
    expect(process.env.PUBLIC_JAZZ_SERVER_URL).toBe(`http://127.0.0.1:${port}`);
  }, 30_000);

  it("exposes top-level telemetry options and starts server-side telemetry", async () => {
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    const startSpy = vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000062",
      port: 19882,
      url: "http://127.0.0.1:19882",
      dataDir: undefined as unknown as string,
      adminSecret: "test-admin-secret",
      backendSecret: "test-backend-secret",
      stop: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(catalogueProject, "deploy").mockResolvedValue(deployed("abc"));
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const root = await tempRoots.create("jazz-sveltekit-top-level-telemetry-test-");
    const plugin = jazzSvelteKit({
      server: { port: 19882, adminSecret: "sveltekit-telemetry-admin" },
      telemetry: "http://127.0.0.1:54418",
    });
    const viteServer = makeViteServer("serve", root);
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(viteServer);

    const startOptions = startSpy.mock.calls[0]![0] as Record<string, unknown>;
    expect(startOptions.telemetryCollectorUrl).toBe("http://127.0.0.1:54418");
    expect(viteServer.config.env!.PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL).toBe(
      "http://127.0.0.1:54418",
    );
    expect(logSpy).toHaveBeenCalledWith("[jazz] telemetry collector: http://127.0.0.1:54418");
  });

  it("is enforce:'pre' so its config hook precedes SvelteKit's env capture", () => {
    expect(jazzSvelteKit().enforce).toBe("pre");
  });

  it("config hook populates process.env with PUBLIC_JAZZ_* before SvelteKit captures env", async () => {
    const persistedAppId = "00000000-0000-0000-0000-000000000099";
    vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: persistedAppId,
      port: 19990,
      url: "http://127.0.0.1:19990",
      dataDir: undefined as unknown as string,
      adminSecret: "test-admin-secret",
      backendSecret: "test-backend-secret",
      stop: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(catalogueProject, "deploy").mockResolvedValue(deployed("abc"));
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const root = await tempRoots.create("jazz-sveltekit-config-hook-");
    const plugin = jazzSvelteKit({
      appId: persistedAppId,
      server: { port: 19990, adminSecret: "config-hook-admin" },
    });

    const config = plugin.config as (
      c: Record<string, unknown>,
      e?: { command: "serve" | "build"; mode?: string },
    ) => unknown;
    const merged = (await config({ root }, { command: "serve", mode: "development" })) as {
      ssr?: { external?: string[] };
    };

    // The merged Vite config is still returned for Vite to consume.
    expect(merged.ssr?.external).toContain("jazz-napi");
    // …and the env is populated by the time the config hook resolves, so
    // SvelteKit's later config({order:'pre'}) capture sees it on the first pass.
    expect(process.env.PUBLIC_JAZZ_APP_ID).toBe(persistedAppId);
    expect(process.env.PUBLIC_JAZZ_SERVER_URL).toBe("http://127.0.0.1:19990");
  });

  it("uses the config hook's resolved shared envDir in production serve mode", async () => {
    const appId = "00000000-0000-0000-0000-000000000122";
    const monorepoRoot = await tempRoots.create("jazz-sveltekit-mode-env-test-");
    const root = join(monorepoRoot, "apps", "web");
    const sharedEnvDir = join(monorepoRoot, "env");
    await mkdir(join(root, "src", "lib"), { recursive: true });
    await mkdir(sharedEnvDir, { recursive: true });
    await writeFile(join(root, "src", "lib", "schema.ts"), todoSchema());
    await writeFile(join(root, "src", "lib", "permissions.ts"), "export default {};\n");
    await writeFile(
      join(sharedEnvDir, ".env.production.local"),
      [
        `PUBLIC_JAZZ_APP_ID=${appId}`,
        "PUBLIC_JAZZ_SERVER_URL=http://production.example",
        "JAZZ_ADMIN_SECRET=production-admin",
        "",
      ].join("\n"),
    );
    const startSpy = vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId,
      port: 19989,
      url: "http://127.0.0.1:19989",
      dataDir: undefined as unknown as string,
      adminSecret: "local-admin",
      backendSecret: "local-backend",
      stop: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(catalogueProject, "deploy").mockResolvedValue(deployed());
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const plugin = jazzSvelteKit();
    const config = plugin.config as (
      c: Record<string, unknown>,
      e?: { command: "serve" | "build"; mode?: string },
    ) => unknown;
    await config({ root, envDir: "../../env" }, { command: "serve", mode: "production" });

    expect(startSpy).not.toHaveBeenCalled();
    expect(process.env.PUBLIC_JAZZ_APP_ID).toBe(appId);
    expect(process.env.PUBLIC_JAZZ_SERVER_URL).toBe("http://production.example");
  });

  it("does not load env files when the config hook receives envFile:false", async () => {
    const root = await tempRoots.create("jazz-sveltekit-env-file-disabled-test-");
    await mkdir(join(root, "src", "lib"), { recursive: true });
    await writeFile(join(root, "src", "lib", "schema.ts"), todoSchema());
    await writeFile(join(root, "src", "lib", "permissions.ts"), "export default {};\n");
    await writeFile(
      join(root, ".env"),
      [
        "PUBLIC_JAZZ_APP_ID=00000000-0000-0000-0000-000000000126",
        "PUBLIC_JAZZ_SERVER_URL=http://should-not-load.example",
        "JAZZ_ADMIN_SECRET=should-not-load",
        "",
      ].join("\n"),
    );
    const startSpy = vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000127",
      port: 19988,
      url: "http://127.0.0.1:19988",
      dataDir: undefined as unknown as string,
      adminSecret: "local-admin",
      backendSecret: "local-backend",
      stop: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(catalogueProject, "deploy").mockResolvedValue(deployed());
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const plugin = jazzSvelteKit();
    const config = plugin.config as (
      c: Record<string, unknown>,
      e?: { command: "serve" | "build"; mode?: string },
    ) => unknown;
    await config({ root, envFile: false }, { command: "serve", mode: "development" });

    expect(startSpy).toHaveBeenCalledOnce();
  });

  it("never restarts the dev server (env is injected in the config hook instead)", async () => {
    vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000098",
      port: 19991,
      url: "http://127.0.0.1:19991",
      dataDir: undefined as unknown as string,
      adminSecret: "test-admin-secret",
      backendSecret: "test-backend-secret",
      stop: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(catalogueProject, "deploy").mockResolvedValue(deployed("abc"));
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const root = await tempRoots.create("jazz-sveltekit-no-restart-");
    const plugin = jazzSvelteKit({
      server: { port: 19991, adminSecret: "no-restart-admin" },
    });
    const config = plugin.config as (
      c: Record<string, unknown>,
      e?: { command: "serve" | "build"; mode?: string },
    ) => unknown;
    await config({ root }, { command: "serve", mode: "development" });

    const viteServer = makeViteServer("serve", root);
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(viteServer);

    expect(viteServer.restart).not.toHaveBeenCalled();
    expect(viteServer.config.env!.PUBLIC_JAZZ_SERVER_URL).toBe("http://127.0.0.1:19991");
  });

  it("does not start a server during build", async () => {
    const spy = vi.spyOn(devServer, "startLocalJazzServer");

    const plugin = jazzSvelteKit({
      server: { port: 19999, adminSecret: "build-admin" },
    });
    const viteServer = makeViteServer("build");
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(viteServer);

    expect(spy).not.toHaveBeenCalled();
    expect(process.env.PUBLIC_JAZZ_APP_ID).toBeUndefined();
    expect(viteServer.config.env?.VITE_JAZZ_INSPECTOR).toBeUndefined();
    expect(viteServer.middlewareHandlers).toHaveLength(0);
  });

  it("does not start a server when server:false", async () => {
    const spy = vi.spyOn(devServer, "startLocalJazzServer");

    const plugin = jazzSvelteKit({ server: false });
    const viteServer = makeViteServer("serve");
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(viteServer);

    expect(spy).not.toHaveBeenCalled();
    expect(process.env.PUBLIC_JAZZ_APP_ID).toBeUndefined();
    expect(viteServer.config.env?.VITE_JAZZ_INSPECTOR).toBeUndefined();
    expect(viteServer.middlewareHandlers).toHaveLength(0);
  });

  it("does not install the inspector overlay when inspector:false", async () => {
    vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000238",
      port: 19986,
      url: "http://127.0.0.1:19986",
      dataDir: undefined as unknown as string,
      adminSecret: "local-admin",
      backendSecret: "local-backend",
      stop: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(catalogueProject, "deploy").mockResolvedValue(deployed("bug238"));
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const plugin = jazzSvelteKit({
      inspector: false,
      server: { port: 19986, adminSecret: "bug238-admin" },
    });
    const root = await tempRoots.create("jazz-sveltekit-no-inspector-test-");
    const viteServer = makeViteServer("serve", root);
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(viteServer);

    expect(viteServer.config.env?.VITE_JAZZ_INSPECTOR).toBeUndefined();
    expect(viteServer.middlewareHandlers).toHaveLength(0);
    await (plugin.closeBundle as () => Promise<void>)();
  });

  it("injects BACKEND_SECRET from the server handle", async () => {
    vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000001",
      port: 19998,
      url: "http://127.0.0.1:19998",
      dataDir: undefined as unknown as string,
      adminSecret: "test-admin-secret",
      backendSecret: "test-backend-secret",
      stop: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(catalogueProject, "deploy").mockResolvedValue(deployed("abc"));
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const plugin = jazzSvelteKit({
      server: { port: 19998, adminSecret: "backend-secret-admin" },
    });
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(makeViteServer("serve"));

    expect(process.env.BACKEND_SECRET).toBe("test-backend-secret");
  });

  it("builds jwksUrl from Vite's configured host and port", async () => {
    const startSpy = vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000004",
      port: 19995,
      url: "http://127.0.0.1:19995",
      dataDir: undefined as unknown as string,
      adminSecret: "test-admin-secret",
      backendSecret: "test-backend-secret",
      stop: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(catalogueProject, "deploy").mockResolvedValue(deployed("abc"));
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const root = await tempRoots.create("jazz-sveltekit-jwks-test-");
    const plugin = jazzSvelteKit({
      server: { port: 19995, adminSecret: "jwks-admin" },
    });
    const viteServer: ViteDevServer = {
      config: { root, command: "serve", env: {}, server: { port: 3000 } },
      httpServer: { once() {} },
      ws: { send() {} },
      restart: vi.fn(),
    };
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(viteServer);

    expect(startSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        jwksUrl: "http://localhost:3000/api/auth/jwks",
      }),
    );
  });

  it("respects APP_ORIGIN when set, over Vite's configured port", async () => {
    const originalAppOrigin = process.env.APP_ORIGIN;
    process.env.APP_ORIGIN = "https://app.example.com";

    const startSpy = vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000005",
      port: 19994,
      url: "http://127.0.0.1:19994",
      dataDir: undefined as unknown as string,
      adminSecret: "test-admin-secret",
      backendSecret: "test-backend-secret",
      stop: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(catalogueProject, "deploy").mockResolvedValue(deployed("abc"));
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    try {
      const root = await tempRoots.create("jazz-sveltekit-apporigin-test-");
      const plugin = jazzSvelteKit({
        server: { port: 19994, adminSecret: "app-origin-admin" },
      });
      const viteServer: ViteDevServer = {
        config: { root, command: "serve", env: {}, server: { port: 3000 } },
        httpServer: { once() {} },
        ws: { send() {} },
        restart: vi.fn(),
      };
      await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(viteServer);

      expect(startSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          jwksUrl: "https://app.example.com/api/auth/jwks",
        }),
      );
    } finally {
      if (originalAppOrigin === undefined) {
        delete process.env.APP_ORIGIN;
      } else {
        process.env.APP_ORIGIN = originalAppOrigin;
      }
    }
  });

  it("connects to an existing server via PUBLIC_JAZZ_SERVER_URL env var", async () => {
    process.env.PUBLIC_JAZZ_SERVER_URL = "http://jazz-test-server:4000";
    process.env.PUBLIC_JAZZ_APP_ID = "00000000-0000-0000-0000-000000000010";

    vi.spyOn(devServer, "startLocalJazzServer");
    vi.spyOn(catalogueProject, "deploy").mockResolvedValue(deployed("abc"));
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const plugin = jazzSvelteKit({ adminSecret: "env-test-admin" });
    const viteServer = makeViteServer("serve");
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(viteServer);

    expect(devServer.startLocalJazzServer).not.toHaveBeenCalled();
    expect(catalogueProject.deploy).toHaveBeenCalledWith(
      expect.objectContaining({
        serverUrl: "http://jazz-test-server:4000",
        appId: "00000000-0000-0000-0000-000000000010",
      }),
    );
    expect(viteServer.config.env!.PUBLIC_JAZZ_SERVER_URL).toBe("http://jazz-test-server:4000");
  });

  it("connects to an existing server via options.server string URL", async () => {
    vi.spyOn(devServer, "startLocalJazzServer");
    vi.spyOn(catalogueProject, "deploy").mockResolvedValue(deployed("abc"));
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const plugin = jazzSvelteKit({
      server: "http://explicit-server:5000",
      adminSecret: "str-admin",
      appId: "00000000-0000-0000-0000-000000000020",
    });
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(makeViteServer("serve"));

    expect(devServer.startLocalJazzServer).not.toHaveBeenCalled();
    expect(catalogueProject.deploy).toHaveBeenCalledWith(
      expect.objectContaining({
        serverUrl: "http://explicit-server:5000",
        appId: "00000000-0000-0000-0000-000000000020",
      }),
    );
  });

  it("ignores a bare server URL env var with no adminSecret and starts a fresh local server", async () => {
    // Simulates the env being populated by a prior initialize() call in the
    // same process (Vite HMR restarts). A bare env var is our own leftover,
    // not a request to connect externally.
    process.env.PUBLIC_JAZZ_SERVER_URL = "http://jazz-test-server:4000";
    process.env.PUBLIC_JAZZ_APP_ID = "00000000-0000-0000-0000-000000000010";

    const port = await getAvailablePort();
    const root = await tempRoots.create("jazz-sveltekit-fallback-");
    await mkdir(join(root, "src", "lib"), { recursive: true });
    await writeFile(join(root, "src", "lib", "schema.ts"), todoSchema());
    await writeFile(join(root, "src", "lib", "permissions.ts"), "export default {};\n");

    const plugin = jazzSvelteKit({ server: { port } });
    const viteServer = makeViteServer("serve", root);
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(viteServer);

    expect(viteServer.config.env!.PUBLIC_JAZZ_SERVER_URL).toBe(`http://127.0.0.1:${port}`);
  }, 30_000);

  it("throws when connecting to an existing server without appId", async () => {
    process.env.PUBLIC_JAZZ_SERVER_URL = "http://jazz-test-server:4000";
    delete process.env.PUBLIC_JAZZ_APP_ID;

    const plugin = jazzSvelteKit({ adminSecret: "admin" });
    await expect(
      (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(makeViteServer("serve")),
    ).rejects.toThrow("appId is required when connecting to an existing server");
  });

  it("sends a full-reload to the browser on a successful schema watch push", async () => {
    vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000050",
      port: 19890,
      url: "http://127.0.0.1:19890",
      dataDir: undefined as unknown as string,
      adminSecret: "test-admin-secret",
      backendSecret: "test-backend-secret",
      stop: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(catalogueProject, "deploy").mockResolvedValue(deployed("abc123def4567890"));
    let capturedOnPush: ((hash: string) => void) | undefined;
    vi.spyOn(schemaWatcher, "watchSchema").mockImplementation((opts) => {
      capturedOnPush = opts.onPush;
      return { close: vi.fn() };
    });

    const root = await tempRoots.create("jazz-sveltekit-reload-test-");
    const wsSend = vi.fn();
    const viteServer: ViteDevServer & { restart: ReturnType<typeof vi.fn> } = {
      config: { root, command: "serve", env: {} },
      httpServer: { once() {} },
      ws: { send: wsSend },
      restart: vi.fn(() => Promise.resolve()),
    };

    const plugin = jazzSvelteKit({
      server: { port: 19890, adminSecret: "reload-admin" },
    });
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(viteServer);

    expect(capturedOnPush).toBeDefined();
    capturedOnPush!("abc123def4567890");

    expect(wsSend).toHaveBeenCalledWith({ type: "full-reload" });
  });

  it("surfaces schema push failures as HMR errors", async () => {
    vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000003",
      port: 19996,
      url: "http://127.0.0.1:19996",
      dataDir: undefined as unknown as string,
      adminSecret: "test-admin-secret",
      backendSecret: "test-backend-secret",
      stop: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(catalogueProject, "deploy").mockRejectedValue(new Error("schema push failed"));
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const root = await tempRoots.create("jazz-sveltekit-hmr-test-");
    const wsSend = vi.fn();
    const viteServer: ViteDevServer = {
      config: { root, command: "serve", env: {} },
      httpServer: { once() {} },
      ws: { send: wsSend },
    };

    const plugin = jazzSvelteKit({
      server: { port: 19996, adminSecret: "hmr-error-admin" },
    });
    const configureServer = plugin.configureServer as (s: ViteDevServer) => Promise<void>;

    await expect(configureServer(viteServer)).rejects.toThrow("schema push failed");
    expect(wsSend).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "error",
        err: expect.objectContaining({
          message: expect.stringContaining("schema push failed"),
        }),
      }),
    );
  });
});

it("config hook adds jazz-napi to ssr.external", () => {
  const plugin = jazzSvelteKit();
  const config = (plugin as { config?: (c: Record<string, unknown>) => unknown }).config;
  expect(config).toBeDefined();
  const result = config!({}) as { ssr?: { external?: string[] } };
  expect(result.ssr?.external).toContain("jazz-napi");
});

it("config hook preserves existing ssr.external entries", () => {
  const plugin = jazzSvelteKit();
  const config = (plugin as { config?: (c: Record<string, unknown>) => unknown }).config;
  const result = config!({ ssr: { external: ["some-other-pkg"] } }) as {
    ssr?: { external?: string[] };
  };
  expect(result.ssr?.external).toContain("jazz-napi");
  expect(result.ssr?.external).toContain("some-other-pkg");
});

it("config hook injects optimizeDeps exclude", () => {
  const plugin = jazzSvelteKit();
  const config = (plugin as { config?: (c: Record<string, unknown>) => unknown }).config;
  const result = config!({}) as {
    optimizeDeps?: { exclude?: string[] };
  };
  expect(result.optimizeDeps?.exclude).toContain("jazz-wasm");
});

it("config hook preserves existing optimizeDeps excludes", () => {
  const plugin = jazzSvelteKit();
  const config = (plugin as { config?: (c: Record<string, unknown>) => unknown }).config;
  const result = config!({ optimizeDeps: { exclude: ["some-dep"] } }) as {
    optimizeDeps?: { exclude?: string[] };
  };
  expect(result.optimizeDeps?.exclude).toContain("jazz-wasm");
  expect(result.optimizeDeps?.exclude).toContain("some-dep");
});

// Without this alias, a pnpm-installed SvelteKit app hits
// "Failed to resolve import 'jazz-wasm'" at runtime — the bare specifier
// in jazz-tools' chunk can't be found unless jazz-wasm is hoisted or a
// direct dep. The alias resolves it from the plugin's own location.
it("config hook aliases jazz-wasm to an absolute path", () => {
  const plugin = jazzSvelteKit();
  const config = (plugin as { config?: (c: Record<string, unknown>) => unknown }).config;
  const result = config!({}) as {
    resolve?: { alias?: { find: RegExp | string; replacement: string }[] };
  };
  const alias = result.resolve?.alias?.find((a) => String(a.find) === "/^jazz-wasm$/");
  expect(alias).toBeDefined();
  expect(alias!.replacement).toMatch(/jazz_wasm\.js$/);
});

describe("dev barrel", () => {
  it("exposes jazzSvelteKit", () => {
    expect((dev as Record<string, unknown>).jazzSvelteKit).toBeDefined();
  });
});
