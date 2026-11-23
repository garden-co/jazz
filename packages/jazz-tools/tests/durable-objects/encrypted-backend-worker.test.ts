import { dirname, join } from "node:path";
import { Miniflare } from "miniflare";
import { afterEach, expect, it } from "vitest";
import { createJazzClient } from "../../src/backend/index.js";
import { memoryPrivateStore } from "../../src/encryption/index.js";
import { startLocalJazzServer } from "../../src/dev/dev-server.js";
import { app } from "../fixtures/encrypted/jazz.generated.js";
import permissions from "../fixtures/encrypted/jazz.permissions.json";
import { bundleWorker } from "./bundle.js";
import { workerModules } from "./harness.js";

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!();
});

it("relays Alice's encrypted title to Bob while the Worker's own reads and writes are keyless", async () => {
  const core = await startLocalJazzServer({ appId: crypto.randomUUID(), inMemory: true });
  cleanups.push(() => core.stop());
  const script = await bundleWorker(join(__dirname, "encrypted-backend-worker.ts"));
  const worker = new Miniflare({
    modulesRoot: dirname(script),
    modules: workerModules(script),
    compatibilityDate: "2026-04-01",
    compatibilityFlags: ["nodejs_compat"],
    durableObjects: { JAZZ_BACKEND: { className: "JazzBackend", useSQLite: true } },
    bindings: { CORE_URL: core.url, APP_ID: core.appId, CORE_SECRET: core.adminSecret },
    host: "127.0.0.1",
    port: 0,
  });
  cleanups.push(() => worker.dispose());
  const url = (await worker.ready).origin;
  const ready = await fetch(url);
  expect(ready.status, await ready.text()).toBe(200);

  async function device() {
    let saved: string | null = null;
    const session = await createJazzClient({
      serverUrl: url + "/jazz",
      appId: core.appId,
      app,
      permissions,
      initial: "device",
      driver: { type: "memory" },
      encryption: { store: memoryPrivateStore() },
      store: {
        read: async () => saved,
        update: async (transform) => {
          saved = transform(saved);
        },
      },
    });
    cleanups.push(() => session.close());
    expect(await session.db.encryption.ready({ timeoutMs: 20_000 })).toBe("active");
    return { db: session.db, account: session.getSnapshot().user!.accountId };
  }

  const alice = await device();
  const bob = await device();
  const project = crypto.randomUUID();
  const space = { table: "projects", id: project };
  const owner = await alice.db.encryption.members.row(space, alice.account);
  await (
    await alice.db.transaction((tx) => {
      tx.insert(app.projects, { name: "Plans", ownerId: alice.account }, { id: project });
      tx.insert(app.__e2ee_members, owner);
    })
  ).wait({ tier: "global" });
  await alice.db
    .insert(app.__e2ee_members, await alice.db.encryption.members.row(space, bob.account))
    .wait({ tier: "global" });
  const taskId = crypto.randomUUID();
  await alice.db
    .insert(
      app.tasks,
      { projectId: project, title: "private title", label: "public label" },
      { id: taskId },
    )
    .wait({ tier: "global" });
  await alice.db.encryption.keys.deliver();
  await expect
    .poll(async () => (await bob.db.all(app.tasks, { tier: "remote" })).map((row) => row.title), {
      timeout: 20_000,
    })
    .toEqual(["private title"]);

  const read = await fetch(url + "/tasks");
  expect(read.status).toBe(200);
  const rows = await read.json();
  expect(rows).toEqual([expect.objectContaining({ id: taskId, label: "public label" })]);
  expect(rows[0]).not.toHaveProperty("title");
  const write = await fetch(url + "/tasks", { method: "POST", body: project });
  expect(write.status).toBe(500);
  expect(await write.text()).toContain(
    "the backend holds no encryption keys; encrypted columns are written by clients (`tasks.title`)",
  );
});
