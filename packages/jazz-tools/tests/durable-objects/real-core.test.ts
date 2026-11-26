// createJazz on a real Jazz core in a Durable Object (the jazz-wasm host),
// in local workerd: publication, writes and reads through the backend, and
// the object's data across a workerd restart. The core never holds a
// transaction open, so nothing here opens one on it.
import { mkdtemp, readdir, rm } from "node:fs/promises";
import { DatabaseSync } from "node:sqlite";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createJazz, durableObjects, type Jazz } from "../../src/backend/index.js";
import { testApp } from "../../src/testing/index.js";
import { bundleWorker } from "./bundle.js";
import { hostBuilt } from "./host.js";
import { startDurableObjectCore, type DurableObjectCore } from "./harness.js";

const app = testApp({ tables: { notes: { columns: { title: "string" } } } });
const permissions = { format: 1, tables: { notes: { read: "true", insert: "true" } } };

describe.skipIf(!hostBuilt && !process.env.CI)("createJazz on a Durable Object core", () => {
  let script: string;
  let persist: string;
  const open: Array<{ core: DurableObjectCore; jazz?: Jazz }> = [];

  /** The Worker, and createJazz on it unless `jazz` is false. */
  async function start(jazz = true) {
    const core = await startDurableObjectCore({ script, persist });
    const entry: { core: DurableObjectCore; jazz?: Jazz } = { core };
    open.push(entry);
    if (!jazz) return entry as { core: DurableObjectCore; jazz: Jazz };
    entry.jazz = createJazz({
      app,
      permissions,
      store: durableObjects({ url: core.url, secret: core.secret }),
    });
    await entry.jazz.ready();
    return entry as { core: DurableObjectCore; jazz: Jazz };
  }

  async function stop() {
    for (const { core, jazz } of open.splice(0)) {
      await jazz?.close().catch(() => undefined);
      await core.dispose();
    }
  }

  beforeAll(async () => {
    script = await bundleWorker(join(__dirname, "real-core-worker.ts"));
    persist = await mkdtemp(join(tmpdir(), "jazz-do-persist-"));
  });
  afterAll(async () => {
    await stop();
    await rm(persist, { recursive: true, force: true });
  });

  it("answers /health, and 426 to the socket route without an upgrade", async () => {
    const { core } = await start(false);
    expect(await (await fetch(`${core.url}/health`)).text()).toBe("ok");
    expect((await fetch(`${core.url}/apps/${await core.appId()}/ws`)).status).toBe(426);
    await stop();
  });

  it("publishes, writes and reads through the backend", async () => {
    const { jazz } = await start();
    await jazz.db.insert(app.notes, { title: "first" }).wait({ tier: "global" });
    const rows = await jazz.db.all(app.notes);
    expect(rows.map((row) => row.title)).toEqual(["first"]);
    await stop();
  });

  it("keeps the object's data across a restart", async () => {
    const { jazz } = await start();
    expect((await jazz.db.all(app.notes)).map((row) => row.title)).toEqual(["first"]);
    await jazz.db.insert(app.notes, { title: "second" }).wait({ tier: "global" });
    await stop();
    const again = await start();
    expect((await again.jazz.db.all(app.notes)).map((row) => row.title).sort()).toEqual([
      "first",
      "second",
    ]);
    await stop();
  });

  // The commit seq row (jazz#6367) on the Durable Object's own SQLite: a
  // write through the core gives every table, history table and
  // `_jazz_transactions` its two `seq_v1` triggers (the storage bridge
  // accepts them), and the `commit_seq.v1` row is the newest accepted seq
  // the data holds, so a restart reads it instead of scanning history.
  it("keeps the commit seq row and its triggers in the object's SQLite", async () => {
    const { jazz } = await start();
    await jazz.db.insert(app.notes, { title: "third" }).wait({ tier: "global" });
    await stop();
    const files = (await readdir(persist, { recursive: true }))
      .map(String)
      .filter((file) => file.endsWith(".sqlite"));
    const opened = files.map((file) => new DatabaseSync(join(persist, file), { readOnly: true }));
    const stores = opened.filter(
      (db) =>
        db.prepare("SELECT count(*) AS n FROM sqlite_master WHERE name = '_jazz_meta'").get()?.n ===
        1,
    );
    try {
      expect(stores.length).toBeGreaterThan(0);
      for (const db of stores) expectCommitSeqRow(db);
    } finally {
      for (const db of opened) db.close();
    }
  });
});

/** `db` (a core's SQLite) covers every table with its commit seq row. */
function expectCommitSeqRow(db: DatabaseSync) {
  const clocked = (
    db
      .prepare(
        "SELECT m.name AS name FROM sqlite_master AS m WHERE m.type = 'table' " +
          "AND m.name NOT GLOB '_jazz_*' AND m.name NOT GLOB '_cf_*' " +
          "AND EXISTS (SELECT 1 FROM pragma_table_info(m.name) WHERE name = '_clock')",
      )
      .all() as Array<{ name: string }>
  ).map((row) => row.name);
  expect(clocked.length).toBeGreaterThan(0);
  const triggers = new Set(
    (
      db
        .prepare(
          "SELECT name FROM sqlite_master WHERE type = 'trigger' AND name GLOB '*__seq_v1_*'",
        )
        .all() as Array<{ name: string }>
    ).map((row) => row.name),
  );
  for (const table of [...clocked, "_jazz_transactions"]) {
    expect(triggers).toContain(`${table}__seq_v1_ins`);
    expect(triggers).toContain(`${table}__seq_v1_upd`);
  }
  const row = db
    .prepare("SELECT v, typeof(v) AS kind FROM _jazz_meta WHERE k = 'commit_seq.v1'")
    .get() as { v: number | bigint; kind: string } | undefined;
  expect(row?.kind).toBe("integer");
  let newest = 0n;
  for (const table of clocked) {
    const max = db
      .prepare(
        `SELECT MAX(_clock) AS m FROM "${table.replaceAll('"', '""')}" ` +
          "WHERE _clock >= 0 AND _clock < 4503599627370496",
      )
      .get() as { m: number | bigint | null };
    if (max.m !== null && BigInt(max.m) > newest) newest = BigInt(max.m);
  }
  expect(newest).toBeGreaterThan(0n);
  expect(BigInt(row!.v)).toBeGreaterThanOrEqual(newest);
}
