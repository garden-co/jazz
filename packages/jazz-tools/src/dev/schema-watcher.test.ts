import { existsSync } from "node:fs";
import { writeFile } from "node:fs/promises";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { watchSchema } from "./schema-watcher.js";
import { startLocalJazzServer, type LocalJazzServerHandle } from "./dev-server.js";
import {
  createTempRootTracker,
  getAvailablePort,
  todoSchema,
  writeTodoSchema,
} from "./test-helpers.js";

const tempRoots = createTempRootTracker();
let server: LocalJazzServerHandle | null = null;

afterEach(async () => {
  if (server) {
    await server.stop();
    server = null;
  }
  await tempRoots.cleanup();
});

describe("watchSchema", () => {
  it("calls onPush after schema.ts changes", async () => {
    const port = await getAvailablePort();
    const adminSecret = "watcher-test-admin";
    server = await startLocalJazzServer({ port, adminSecret });

    const schemaDir = await tempRoots.create("jazz-watcher-test-");
    await writeTodoSchema(schemaDir);

    const pushPromise = new Promise<string>((resolve) => {
      const watcher = watchSchema({
        schemaDir,
        serverUrl: server!.url,
        appId: server!.appId,
        adminSecret,
        onPush: (hash) => {
          resolve(hash);
          watcher.close();
        },
        onError: (err) => {
          throw err;
        },
      });

      setTimeout(async () => {
        if (!existsSync(schemaDir)) return;
        await writeFile(join(schemaDir, "schema.ts"), todoSchema() + "\n// trigger change\n");
      }, 300);
    });

    const hash = await pushPromise;
    expect(typeof hash).toBe("string");
    expect(hash.length).toBeGreaterThan(0);
  }, 30_000);

  it("calls onError for an invalid schema", async () => {
    const port = await getAvailablePort();
    const adminSecret = "watcher-err-admin";
    server = await startLocalJazzServer({ port, adminSecret });

    const schemaDir = await tempRoots.create("jazz-watcher-err-");
    await writeTodoSchema(schemaDir);

    const errorPromise = new Promise<Error>((resolve) => {
      const watcher = watchSchema({
        schemaDir,
        serverUrl: server!.url,
        appId: server!.appId,
        adminSecret,
        onError: (err) => {
          resolve(err);
          watcher.close();
        },
      });

      setTimeout(async () => {
        if (!existsSync(schemaDir)) return;
        await writeFile(join(schemaDir, "schema.ts"), "export const schema = 'broken';");
      }, 300);
    });

    const err = await errorPromise;
    expect(err).toBeInstanceOf(Error);
  }, 30_000);
});
