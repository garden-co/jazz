import { afterEach, describe, expect, it } from "vitest";
import { migration as m, schema as s } from "../index.js";
import { definePermissions } from "../permissions/index.js";
import { computeSchemaHash } from "../schema-hash.js";
import { appArtifact, appPermissions } from "../testing/app-artifact.js";
import { startLocalJazzServer, type LocalJazzServerHandle } from "./dev-server.js";
import { deployApp } from "./catalogue.js";

// A store whose history doesn't match the app's migrations is refused at
// deploy. The refusal names both histories by the short hashes jazz-tools
// prints and says what to do (#5053).
const v1 = { notes: s.table({ title: s.string() }, {}) };
const v2 = { notes: s.table({ title: s.string(), body: s.string().optional() }, {}) };
const appV1 = s.defineApp(v1, { migrations: "none" });
const appV2 = s.defineApp(v2, { migrations: "none" });
const short = async (app: { wasmSchema: Parameters<typeof computeSchemaHash>[0] }) =>
  (await computeSchemaHash(app.wasmSchema)).slice(0, 12);
// Everyone may read notes, in either version.
const readV1 = definePermissions(appV1, ({ policy }) => {
  policy.notes.allowRead.always();
});
const readV2 = definePermissions(appV2, ({ policy }) => {
  policy.notes.allowRead.always();
});

const servers: LocalJazzServerHandle[] = [];
afterEach(async () => {
  await Promise.all(servers.splice(0).map((server) => server.stop()));
});

async function store() {
  const server = await startLocalJazzServer({ adminSecret: "admin-secret", inMemory: true });
  servers.push(server);
  type Deployed = Parameters<typeof deployApp>[0];
  return (app: Deployed["app"], permissions: Deployed["permissions"]) =>
    deployApp({
      appId: server.appId,
      serverUrl: server.url,
      adminSecret: "admin-secret",
      app,
      permissions,
    });
}

describe("a deploy whose migrations don't match the store's history", () => {
  // `{ migrations: "none" }` after a schema change: the app ships no
  // migration from the version the store runs.
  // The server's graph check refuses this before the store's own check
  // would: its existing "disconnected roots" guidance already names the
  // version and the fix, which this pins.
  it("refuses a deploy with no migration from the stored version, naming it", async () => {
    const deploy = await store();
    await deploy(appV1, readV1);
    const refused = deploy(appV2, readV2);
    await expect(refused).rejects.toThrow(/migration history has disconnected roots: /);
    await expect(refused).rejects.toThrow(await short(appV1));
    await expect(refused).rejects.toThrow("jazz-tools migrations create");
  }, 20_000);

  it("names both first versions when the store started from another one", async () => {
    const deploy = await store();
    await deploy(appV2, readV2);
    const migration = m.defineMigration({
      fromHash: await computeSchemaHash(appV1.wasmSchema),
      toHash: await computeSchemaHash(appV2.wasmSchema),
      from: v1,
      to: v2,
      migrate: { notes: { body: m.add.string({ default: null }) } },
    });
    const migrated = s.defineApp(v2, {
      artifact: await appArtifact(v1, { steps: [{ to: v2, migration }] }),
    });
    await expect(deploy(migrated, await appPermissions(migrated, readV2))).rejects.toThrow(
      `this store's history starts at schema ${await short(appV2)}, but the app's migrations start at ${await short(appV1)}`,
    );
  }, 20_000);
});
