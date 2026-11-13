import { afterEach, describe, expect, it } from "vitest";
import { migration as m, schema as s } from "../index.js";
import { definePermissions } from "../permissions/index.js";
import { computeSchemaHash } from "../schema-hash.js";
import { appMigrations } from "../testing/client-migrations.js";
import { startLocalJazzServer, type LocalJazzServerHandle } from "./dev-server.js";
import { deployApp } from "./catalogue.js";

// A store whose history doesn't match the app's migrations is refused at
// deploy. The refusal names both histories by the short hashes jazz-tools
// prints and says what to do (#5053).
const v1 = { notes: s.table({ title: s.string() }, {}) };
const v2 = { notes: s.table({ title: s.string(), body: s.string().optional() }, {}) };
const unversioned = (schema: typeof v1 | typeof v2) => s.defineApp(schema, { migrations: "none" });
const short = async (schema: typeof v1 | typeof v2) =>
  (await computeSchemaHash(unversioned(schema).wasmSchema)).slice(0, 12);
const allowAll = (app: ReturnType<typeof unversioned>) =>
  definePermissions(app, ({ policy }) => {
    policy.notes.allowRead.always();
  });

const servers: LocalJazzServerHandle[] = [];
afterEach(async () => {
  await Promise.all(servers.splice(0).map((server) => server.stop()));
});

async function store() {
  const server = await startLocalJazzServer({ adminSecret: "admin-secret", inMemory: true });
  servers.push(server);
  return (app: ReturnType<typeof unversioned>) =>
    deployApp({
      appId: server.appId,
      serverUrl: server.url,
      adminSecret: "admin-secret",
      app,
      permissions: allowAll(app),
    });
}

describe("a deploy whose migrations don't match the store's history", () => {
  // `{ migrations: "none" }` after a schema change: the app ships no
  // migration from the version the store runs.
  it("names the version the app ships no migration from, and how to write one", async () => {
    const deploy = await store();
    await deploy(unversioned(v1));
    const refused = deploy(unversioned(v2));
    await expect(refused).rejects.toThrow(/migration history has disconnected roots: /);
    await expect(refused).rejects.toThrow(await short(v1));
    await expect(refused).rejects.toThrow("jazz-tools migrations create");
  }, 20_000);

  it("names both first versions when the store started from another one", async () => {
    const deploy = await store();
    await deploy(unversioned(v2));
    const migration = m.defineMigration({
      fromHash: await computeSchemaHash(unversioned(v1).wasmSchema),
      toHash: await computeSchemaHash(unversioned(v2).wasmSchema),
      from: v1,
      to: v2,
      migrate: { notes: { body: m.add.string({ default: null }) } },
    });
    const migrated = s.defineApp(v2, {
      migrations: await appMigrations(v1, { to: v2, migration }),
    });
    await expect(deploy(migrated)).rejects.toThrow(
      `this store's history starts at schema ${await short(v2)}, but the app's migrations start at ${await short(v1)}`,
    );
  }, 20_000);
});
