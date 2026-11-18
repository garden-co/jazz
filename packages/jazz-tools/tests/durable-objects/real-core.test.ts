// createJazz on a real Jazz core in a Durable Object (the jazz-wasm host),
// in local workerd: publication, writes and reads through the backend, and
// the object's data across a workerd restart. The core never holds a
// transaction open, so nothing here opens one on it.
import { existsSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createJazz, durableObjects, type Jazz } from "../../src/backend/index.js";
import { testApp } from "../../src/testing/index.js";
import { bundleWorker } from "./bundle.js";
import { startDurableObjectCore, type DurableObjectCore } from "./harness.js";

// jazz-wasm's Durable Object build (`pnpm --filter jazz-wasm build:durable-object`).
const hostBuilt = existsSync(
  join(
    dirname(createRequire(import.meta.url).resolve("jazz-wasm/package.json")),
    "durable-object/pkg/jazz_wasm_do_bg.wasm",
  ),
);

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
});
