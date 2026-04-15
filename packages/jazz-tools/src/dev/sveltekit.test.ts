import { afterEach, describe, expect, it, vi } from "vitest";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { createTempRootTracker, getAvailablePort, todoSchema } from "./test-helpers.js";
import * as devServer from "./dev-server.js";
import * as schemaWatcher from "./schema-watcher.js";
import { jazzSvelteKit } from "./sveltekit.js";
import type { ViteDevServer } from "./vite.js";

const dev = await import("./index.js");

const tempRoots = createTempRootTracker();
const originalJazzAppId = process.env.PUBLIC_JAZZ_APP_ID;
const originalJazzServerUrl = process.env.PUBLIC_JAZZ_SERVER_URL;
const originalBackendSecret = process.env.BACKEND_SECRET;

function makeViteServer(
  command: "serve" | "build",
  root = "/tmp/jazz-sveltekit-test",
): ViteDevServer {
  return {
    config: { root, command, env: {} },
    httpServer: { once() {} },
    ws: { send() {} },
  };
}

afterEach(async () => {
  await tempRoots.cleanup();
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

  if (originalBackendSecret === undefined) {
    delete process.env.BACKEND_SECRET;
  } else {
    process.env.BACKEND_SECRET = originalBackendSecret;
  }
});

describe("jazzSvelteKit", () => {
  it("starts a local server in dev and injects PUBLIC_JAZZ_* env vars", async () => {
    const port = await getAvailablePort();
    const root = await tempRoots.create("jazz-sveltekit-test-");
    await mkdir(join(root, "src", "lib"), { recursive: true });
    await writeFile(join(root, "src", "lib", "schema.ts"), todoSchema());

    const plugin = jazzSvelteKit({ server: { port, adminSecret: "sveltekit-test-admin" } });
    const viteServer = makeViteServer("serve", root);
    const configureServer = plugin.configureServer as (server: typeof viteServer) => Promise<void>;
    await configureServer(viteServer);

    const healthResponse = await fetch(`http://127.0.0.1:${port}/health`);
    expect(healthResponse.ok).toBe(true);

    const schemasResponse = await fetch(`http://127.0.0.1:${port}/schemas`, {
      headers: { "X-Jazz-Admin-Secret": "sveltekit-test-admin" },
    });
    expect(schemasResponse.ok).toBe(true);
    const body = (await schemasResponse.json()) as { hashes?: string[] };
    expect(body.hashes?.length).toBeGreaterThan(0);

    expect(viteServer.config.env!.PUBLIC_JAZZ_APP_ID).toBeTruthy();
    expect(viteServer.config.env!.PUBLIC_JAZZ_SERVER_URL).toBe(`http://127.0.0.1:${port}`);
    expect(process.env.PUBLIC_JAZZ_APP_ID).toBe(viteServer.config.env!.PUBLIC_JAZZ_APP_ID);
    expect(process.env.PUBLIC_JAZZ_SERVER_URL).toBe(`http://127.0.0.1:${port}`);
  }, 30_000);

  it("does not start a server during build", async () => {
    const spy = vi.spyOn(devServer, "startLocalJazzServer");

    const plugin = jazzSvelteKit({ server: { port: 19999, adminSecret: "build-admin" } });
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(makeViteServer("build"));

    expect(spy).not.toHaveBeenCalled();
    expect(process.env.PUBLIC_JAZZ_APP_ID).toBeUndefined();
  });

  it("does not start a server when server:false", async () => {
    const spy = vi.spyOn(devServer, "startLocalJazzServer");

    const plugin = jazzSvelteKit({ server: false });
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(makeViteServer("serve"));

    expect(spy).not.toHaveBeenCalled();
    expect(process.env.PUBLIC_JAZZ_APP_ID).toBeUndefined();
  });

  it("injects BACKEND_SECRET from the server handle", async () => {
    vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000001",
      port: 19998,
      url: "http://127.0.0.1:19998",
      dataDir: undefined as unknown as string,
      backendSecret: "test-backend-secret",
      stop: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(devServer, "pushSchemaCatalogue").mockResolvedValue({ hash: "abc" });
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const plugin = jazzSvelteKit({ server: { port: 19998, adminSecret: "backend-secret-admin" } });
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(makeViteServer("serve"));

    expect(process.env.BACKEND_SECRET).toBe("test-backend-secret");
  });

  it("builds jwksUrl from Vite's configured host and port", async () => {
    const startSpy = vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000004",
      port: 19995,
      url: "http://127.0.0.1:19995",
      dataDir: undefined as unknown as string,
      stop: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(devServer, "pushSchemaCatalogue").mockResolvedValue({ hash: "abc" });
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const root = await tempRoots.create("jazz-sveltekit-jwks-test-");
    const plugin = jazzSvelteKit({ server: { port: 19995, adminSecret: "jwks-admin" } });
    const viteServer: ViteDevServer = {
      config: { root, command: "serve", env: {}, server: { port: 3000 } },
      httpServer: { once() {} },
      ws: { send() {} },
    };
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(viteServer);

    expect(startSpy).toHaveBeenCalledWith(
      expect.objectContaining({ jwksUrl: "http://localhost:3000/api/auth/jwks" }),
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
      stop: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(devServer, "pushSchemaCatalogue").mockResolvedValue({ hash: "abc" });
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    try {
      const root = await tempRoots.create("jazz-sveltekit-apporigin-test-");
      const plugin = jazzSvelteKit({ server: { port: 19994, adminSecret: "app-origin-admin" } });
      const viteServer: ViteDevServer = {
        config: { root, command: "serve", env: {}, server: { port: 3000 } },
        httpServer: { once() {} },
        ws: { send() {} },
      };
      await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(viteServer);

      expect(startSpy).toHaveBeenCalledWith(
        expect.objectContaining({ jwksUrl: "https://app.example.com/api/auth/jwks" }),
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
    vi.spyOn(devServer, "pushSchemaCatalogue").mockResolvedValue({ hash: "abc" });
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const plugin = jazzSvelteKit({ adminSecret: "env-test-admin" });
    const viteServer = makeViteServer("serve");
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(viteServer);

    expect(devServer.startLocalJazzServer).not.toHaveBeenCalled();
    expect(devServer.pushSchemaCatalogue).toHaveBeenCalledWith(
      expect.objectContaining({
        serverUrl: "http://jazz-test-server:4000",
        appId: "00000000-0000-0000-0000-000000000010",
      }),
    );
    expect(viteServer.config.env!.PUBLIC_JAZZ_SERVER_URL).toBe("http://jazz-test-server:4000");
  });

  it("connects to an existing server via options.server string URL", async () => {
    vi.spyOn(devServer, "startLocalJazzServer");
    vi.spyOn(devServer, "pushSchemaCatalogue").mockResolvedValue({ hash: "abc" });
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const plugin = jazzSvelteKit({
      server: "http://explicit-server:5000",
      adminSecret: "str-admin",
      appId: "00000000-0000-0000-0000-000000000020",
    });
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(makeViteServer("serve"));

    expect(devServer.startLocalJazzServer).not.toHaveBeenCalled();
    expect(devServer.pushSchemaCatalogue).toHaveBeenCalledWith(
      expect.objectContaining({
        serverUrl: "http://explicit-server:5000",
        appId: "00000000-0000-0000-0000-000000000020",
      }),
    );
  });

  it("throws when connecting to an existing server without adminSecret", async () => {
    process.env.PUBLIC_JAZZ_SERVER_URL = "http://jazz-test-server:4000";
    process.env.PUBLIC_JAZZ_APP_ID = "00000000-0000-0000-0000-000000000010";

    const plugin = jazzSvelteKit({});
    await expect(
      (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(makeViteServer("serve")),
    ).rejects.toThrow("adminSecret is required when connecting to an existing server");
  });

  it("throws when connecting to an existing server without appId", async () => {
    process.env.PUBLIC_JAZZ_SERVER_URL = "http://jazz-test-server:4000";
    delete process.env.PUBLIC_JAZZ_APP_ID;

    const plugin = jazzSvelteKit({ adminSecret: "admin" });
    await expect(
      (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(makeViteServer("serve")),
    ).rejects.toThrow("appId is required when connecting to an existing server");
  });

  it("stops the server and closes the watcher when the close hook fires", async () => {
    const stop = vi.fn().mockResolvedValue(undefined);
    const close = vi.fn();

    vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000002",
      port: 19997,
      url: "http://127.0.0.1:19997",
      dataDir: undefined as unknown as string,
      stop,
    });
    vi.spyOn(devServer, "pushSchemaCatalogue").mockResolvedValue({ hash: "abc" });
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close });

    const root = await tempRoots.create("jazz-sveltekit-close-test-");
    let capturedCloseCallback: (() => void) | undefined;
    const viteServer: ViteDevServer = {
      config: { root, command: "serve", env: {} },
      httpServer: {
        once(event, cb) {
          if (event === "close") capturedCloseCallback = cb;
        },
      },
      ws: { send() {} },
    };

    const plugin = jazzSvelteKit({ server: { port: 19997, adminSecret: "close-hook-admin" } });
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(viteServer);

    expect(capturedCloseCallback).toBeDefined();
    capturedCloseCallback!();
    await new Promise((r) => setTimeout(r, 50));

    expect(stop).toHaveBeenCalledOnce();
    expect(close).toHaveBeenCalledOnce();
  });

  it("surfaces schema push failures as HMR errors", async () => {
    vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000003",
      port: 19996,
      url: "http://127.0.0.1:19996",
      dataDir: undefined as unknown as string,
      stop: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(devServer, "pushSchemaCatalogue").mockRejectedValue(new Error("schema push failed"));
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const root = await tempRoots.create("jazz-sveltekit-hmr-test-");
    const wsSend = vi.fn();
    const viteServer: ViteDevServer = {
      config: { root, command: "serve", env: {} },
      httpServer: { once() {} },
      ws: { send: wsSend },
    };

    const plugin = jazzSvelteKit({ server: { port: 19996, adminSecret: "hmr-error-admin" } });
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

describe("dev barrel", () => {
  it("exposes jazzSvelteKit", () => {
    expect((dev as Record<string, unknown>).jazzSvelteKit).toBeDefined();
  });
});
