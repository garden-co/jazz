import { existsSync } from "node:fs";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { setTimeout as delay } from "node:timers/promises";
import { deploy } from "./catalogue-project.js";
import { loadCompiledSchema } from "../schema-loader.js";
import { computeSchemaHash } from "../schema-hash.js";
import { watchSchema } from "./schema-watcher.js";
import { startLocalJazzServer, type LocalJazzServerHandle } from "./dev-server.js";
import {
  createTempRootTracker,
  getAvailablePort,
  todoSchema,
  writeTodoSchema,
} from "./test-helpers.js";

const tempRoots = createTempRootTracker();
// Deployment includes debouncing, schema/permissions compilation, and server requests.
const DEPLOY_TIMEOUT = 10_000;
let server: LocalJazzServerHandle | null = null;

afterEach(async () => {
  if (server) {
    await server.stop();
    server = null;
  }
  await tempRoots.cleanup();
});

async function writeWatcherApp(schemaDir: string) {
  await writeTodoSchema(schemaDir);
  await writeFile(
    join(schemaDir, "permissions.ts"),
    `
    import { schema as s } from "jazz-tools";
    import { app } from "./schema.js";
    export default s.definePermissions(app, ({ policy }) => {
      policy.todos.allowRead.always();
      policy.todos.allowInsert.always();
    });
  `,
  );
}

describe("watchSchema", () => {
  for (const existingDirectory of [false, true]) {
    it(`retries deployment when a migration is added and edited in a ${existingDirectory ? "preexisting" : "new"} directory`, async () => {
      server = await startLocalJazzServer({ inMemory: true });
      const schemaDir = await tempRoots.create("jazz-watcher-migration-");
      await writeWatcherApp(schemaDir);
      const options = {
        schemaDir,
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
      };
      const initial = await deploy(options);
      const migrationsDir = join(schemaDir, "migrations");
      await rm(migrationsDir, { recursive: true, force: true });
      if (existingDirectory) await mkdir(migrationsDir);
      const onPush = vi.fn();
      const onError = vi.fn();
      const watcher = watchSchema({ ...options, onPush, onError });
      try {
        await writeFile(
          join(schemaDir, "schema.ts"),
          todoSchema().replace(
            "done: s.boolean(),",
            "done: s.boolean(), tags: s.array(s.string()),",
          ),
        );
        await expect
          .poll(() => onError.mock.calls.at(-1)?.[0].message ?? "", { timeout: DEPLOY_TIMEOUT })
          .toContain("migrations create");
        const targetHash = await computeSchemaHash(
          (await loadCompiledSchema(schemaDir)).wasmSchema,
        );
        await mkdir(migrationsDir, { recursive: true });
        const migrationFile = join(
          migrationsDir,
          `20260917T000000-add-tags-${initial.schema.hash.slice(0, 12)}-${targetHash.slice(0, 12)}.ts`,
        );
        const errorsBefore = onError.mock.calls.length;
        await writeFile(migrationFile, "export default undefined;");
        await expect
          .poll(() => onError.mock.calls.length, { timeout: DEPLOY_TIMEOUT })
          .toBeGreaterThan(errorsBefore);
        await writeFile(
          migrationFile,
          `
          import { schema as s } from "jazz-tools";
          const columns = { title: s.string(), done: s.boolean() };
          export default s.defineMigration({
            fromHash: "${initial.schema.hash}", toHash: "${targetHash}",
            from: { todos: s.table(columns, {}) },
            to: { todos: s.table({ ...columns, tags: s.array(s.string()) }, {}) },
            migrate: { todos: { tags: s.add.array({ of: s.string(), default: [] }) } },
          });
        `,
        );
        await expect
          .poll(() => onPush.mock.calls.at(-1)?.[0], { timeout: DEPLOY_TIMEOUT })
          .toBe(targetHash);
        await mkdir(join(migrationsDir, "snapshots"), { recursive: true });
        const pushes = onPush.mock.calls.length;
        const errors = onError.mock.calls.length;
        await writeFile(join(migrationsDir, "snapshots", "ignored.json"), "{}");
        await delay(500);
        expect(onPush).toHaveBeenCalledTimes(pushes);
        expect(onError).toHaveBeenCalledTimes(errors);
        watcher.close();
        await writeFile(migrationFile, "export default undefined;");
        await delay(500);
        expect(onError).toHaveBeenCalledTimes(errors);
        expect(onPush).toHaveBeenCalledTimes(pushes);
      } finally {
        watcher.close();
      }
    }, 30_000);
  }

  it("calls onPush after schema.ts changes", async () => {
    const port = await getAvailablePort();
    const adminSecret = "watcher-test-admin";
    server = await startLocalJazzServer({ port, adminSecret });

    const schemaDir = await tempRoots.create("jazz-watcher-test-");
    await writeWatcherApp(schemaDir);

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
    await writeWatcherApp(schemaDir);

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
