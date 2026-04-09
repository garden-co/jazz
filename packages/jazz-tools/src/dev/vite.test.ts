import { writeFile } from "node:fs/promises";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { jazzPlugin } from "./vite.js";
import { createTempRootTracker, getAvailablePort, todoSchema } from "./test-helpers.js";

const tempRoots = createTempRootTracker();
const originalJazzServerUrl = process.env.JAZZ_SERVER_URL;
const originalJazzAppId = process.env.JAZZ_APP_ID;

afterEach(async () => {
  await tempRoots.cleanup();

  if (originalJazzServerUrl === undefined) {
    delete process.env.JAZZ_SERVER_URL;
  } else {
    process.env.JAZZ_SERVER_URL = originalJazzServerUrl;
  }

  if (originalJazzAppId === undefined) {
    delete process.env.JAZZ_APP_ID;
    return;
  }

  process.env.JAZZ_APP_ID = originalJazzAppId;
});

describe("jazzPlugin", () => {
  it("returns a Vite plugin with the correct name", () => {
    const plugin = jazzPlugin();
    expect(plugin.name).toBe("jazz");
  });

  it("starts a server and pushes schema via configureServer hook", async () => {
    const port = await getAvailablePort();
    const schemaDir = await tempRoots.create("jazz-vite-test-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());

    const plugin = jazzPlugin({
      server: { port, adminSecret: "vite-test-admin" },
      schemaDir,
    });

    const closeHandlers: (() => Promise<void> | void)[] = [];
    const fakeViteServer = {
      config: { root: schemaDir, command: "serve" as const, env: {} as Record<string, string> },
      httpServer: {
        once(_event: string, cb: () => void) {
          closeHandlers.push(cb);
        },
      },
      ws: { send() {} },
    };

    const configureServer = plugin.configureServer as (
      server: typeof fakeViteServer,
    ) => Promise<void>;
    await configureServer(fakeViteServer);

    const healthResponse = await fetch(`http://127.0.0.1:${port}/health`);
    expect(healthResponse.ok).toBe(true);

    const schemasResponse = await fetch(`http://127.0.0.1:${port}/schemas`, {
      headers: { "X-Jazz-Admin-Secret": "vite-test-admin" },
    });
    expect(schemasResponse.ok).toBe(true);
    const body = (await schemasResponse.json()) as { hashes?: string[] };
    expect(body.hashes?.length).toBeGreaterThan(0);
    expect(fakeViteServer.config.env.JAZZ_APP_ID).toBeTruthy();
    expect(fakeViteServer.config.env.JAZZ_SERVER_URL).toBe(`http://127.0.0.1:${port}`);
    expect(process.env.JAZZ_APP_ID).toBe(fakeViteServer.config.env.JAZZ_APP_ID);
    expect(process.env.JAZZ_SERVER_URL).toBe(`http://127.0.0.1:${port}`);

    for (const handler of closeHandlers) {
      await handler();
    }

    await expect(fetch(`http://127.0.0.1:${port}/health`).then((r) => r.ok)).rejects.toThrow();
  }, 30_000);

  it("does not inject a dev server url during build", async () => {
    const plugin = jazzPlugin();
    const fakeViteServer = {
      config: {
        root: "/tmp/jazz-build",
        command: "build" as const,
        env: {} as Record<string, string>,
      },
      httpServer: null,
      ws: { send() {} },
    };

    const configureServer = plugin.configureServer as (
      server: typeof fakeViteServer,
    ) => Promise<void>;
    await configureServer(fakeViteServer);

    expect(fakeViteServer.config.env.JAZZ_APP_ID).toBeUndefined();
    expect(fakeViteServer.config.env.JAZZ_SERVER_URL).toBeUndefined();
    expect(process.env.JAZZ_APP_ID).toBe(originalJazzAppId);
    expect(process.env.JAZZ_SERVER_URL).toBe(originalJazzServerUrl);
  });
});
