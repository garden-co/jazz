import { afterEach, describe, expect, it } from "vitest";
import { testApp } from "../testing/index.js";
import { boundApp } from "../typed-app.js";
import { startLocalJazzServer, type LocalJazzServerHandle } from "./dev-server.js";
import { deployApp } from "./catalogue.js";

// A store whose history doesn't match the app's migrations is refused at
// deploy. The refusal names both histories by the short hashes jazz-tools
// prints and says what to do (#5053).
const v1 = { notes: { columns: { title: "string" } } };
const v2 = { notes: { columns: { title: "string", body: "string?" } } };
const appV1 = testApp({ tables: v1 });
const appV2 = testApp({ tables: v2 });
const short = async (app: typeof appV1) => boundApp(app.wasmSchema)!.schema.current.slice(0, 12);
// Everyone may read notes, in either version.
const readNotes = { format: 1, tables: { notes: { read: "true" } } };

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
  // An app whose schema changed without `migrations create`: it ships no
  // migration from the version the store runs.
  // The server's graph check refuses this before the store's own check
  // would: its existing "disconnected roots" guidance already names the
  // version and the fix, which this pins.
  it("refuses a deploy with no migration from the stored version, naming it", async () => {
    const deploy = await store();
    await deploy(appV1, readNotes);
    const refused = deploy(appV2, readNotes);
    await expect(refused).rejects.toThrow(/migration history has disconnected roots: /);
    await expect(refused).rejects.toThrow(await short(appV1));
    await expect(refused).rejects.toThrow("jazz-tools migrations create");
  }, 20_000);

  it("names both first versions when the store started from another one", async () => {
    const deploy = await store();
    await deploy(appV2, readNotes);
    const migrated = testApp({
      tables: v2,
      history: { v1: { tables: v1 } },
      migrations: [{ from: "v1", migrate: { notes: { body: { add: "string", default: null } } } }],
    });
    await expect(deploy(migrated, readNotes)).rejects.toThrow(
      `this store's history starts at schema ${await short(appV2)}, but the app's migrations start at ${await short(appV1)}`,
    );
  }, 20_000);
});
