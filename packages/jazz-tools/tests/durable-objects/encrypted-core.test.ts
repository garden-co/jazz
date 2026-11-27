// An encrypted app whose core is a Durable Object (the jazz-wasm host), in
// local workerd: the one-Worker setup the docs show (`jazzWorker` with
// `durableObjects({ binding })`). Alice and Bob, two device clients with
// keys, share a project through it; the Worker's backend holds no keys.
//
//   Alice writes encrypted titles ──► Bob reads them
//   the backend sees no `title` and can't write or rotate one
//   Alice removes Bob and rotates ──► Bob is still sent her next write, and can't open it
//   no plaintext in the objects' SQLite or in the R2 bucket of large values
import { readdir, readFile, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { createJazzClient, unreadableRows } from "../../src/backend/index.js";
import { memoryPrivateStore } from "../../src/encryption/index.js";
import { app } from "../fixtures/encrypted/jazz.generated.js";
import permissions from "../fixtures/encrypted/jazz.permissions.json";
import { bundleWorker } from "./bundle.js";
import { hostBuilt } from "./host.js";
import { startDurableObjectCore } from "./harness.js";

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
});

/** Every file under `dir`, recursively. */
async function files(dir: string): Promise<string[]> {
  const entries = await readdir(dir, { recursive: true, withFileTypes: true });
  return entries
    .filter((entry) => entry.isFile())
    .map((entry) => join(entry.parentPath, entry.name));
}

/** Which of `secrets` occur in `bytes`, as UTF-8 or as hex. */
function leaked(bytes: Buffer, secrets: string[]): string[] {
  return secrets.filter(
    (secret) =>
      bytes.includes(Buffer.from(secret)) ||
      bytes.includes(Buffer.from(Buffer.from(secret).toString("hex"))),
  );
}

describe.skipIf(!hostBuilt && !process.env.CI)("an encrypted app on a Durable Object core", () => {
  it("relays Alice's titles to Bob, keeps the backend keyless and stores no plaintext", async () => {
    const persist = await mkdtemp(join(tmpdir(), "jazz-do-encrypted-"));
    cleanups.push(() => rm(persist, { recursive: true, force: true }));
    const script = await bundleWorker(join(__dirname, "encrypted-core-worker.ts"));
    const core = await startDurableObjectCore({
      script,
      backend: true,
      persist,
      secret: null,
      blobs: true,
    });
    let disposed = false;
    cleanups.push(async () => {
      if (!disposed) await core.dispose();
    });
    const { url } = core;
    const started = await fetch(`${url}/app-id`);
    expect(started.status, await started.clone().text()).toBe(200);
    const appId = await started.text();

    async function device() {
      let saved: string | null = null;
      const session = await createJazzClient({
        serverUrl: `${url}/jazz`,
        appId,
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
      return { db: session.db, account: session.getSnapshot().user!.accountId, session };
    }

    const alice = await device();
    const bob = await device();
    const tag = crypto.randomUUID();
    const small = `small title ${tag}`;
    // Over the core's large-value threshold, so its ciphertext moves to R2.
    const large = `large title ${tag} `.repeat(Math.ceil((200 * 1024) / (tag.length + 13)));
    const afterRemoval = `after removal ${tag}`;

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
    const smallId = crypto.randomUUID();
    const largeId = crypto.randomUUID();
    for (const [id, title] of [
      [smallId, small],
      [largeId, large],
    ]) {
      await alice.db
        .insert(app.tasks, { projectId: project, title, label: "public label" }, { id })
        .wait({ tier: "global" });
    }
    await alice.db.encryption.keys.deliver();
    const titles = (rows: Array<{ id: string; title: string }>) =>
      Object.fromEntries(rows.map((row) => [row.id, row.title]));
    await expect
      .poll(async () => titles(await bob.db.all(app.tasks, { tier: "remote" })), {
        timeout: 30_000,
      })
      .toEqual({ [smallId]: small, [largeId]: large });

    // The backend sees the rows without their encrypted column, can't write
    // one, and has no keys to list devices or rotate with.
    const read = await fetch(`${url}/tasks`);
    expect(read.status, await read.clone().text()).toBe(200);
    const rows = (await read.json()) as Array<Record<string, unknown>>;
    expect(rows.map((row) => row.id).sort()).toEqual([smallId, largeId].sort());
    for (const row of rows) {
      expect(row).toMatchObject({ label: "public label" });
      expect(row).not.toHaveProperty("title");
    }
    const write = await fetch(`${url}/tasks`, { method: "POST", body: project });
    expect(write.status).toBe(500);
    expect(await write.text()).toContain(
      "the backend holds no encryption keys; encrypted columns are written by clients (`tasks.title`)",
    );
    expect(await (await fetch(`${url}/keyless?project=${project}`)).json()).toEqual({
      devices: "keyless",
      rotate: "keyless",
    });

    // Alice removes Bob and rotates. Bob is still sent her next write (tasks
    // are readable to all here), but holds no key for it.
    expect(await alice.db.encryption.members.remove(space, bob.account)).toBe(1);
    await alice.db.encryption.keys.rotate(space);
    const afterId = crypto.randomUUID();
    await alice.db
      .insert(app.tasks, { projectId: project, title: afterRemoval }, { id: afterId })
      .wait({ tier: "global" });
    await expect
      .poll(async () => (await alice.db.all(app.tasks.where({ id: afterId }))).map((r) => r.title))
      .toEqual([afterRemoval]);
    let seen: Array<{ id: string }> = [];
    await expect
      .poll(
        async () => {
          seen = await bob.db.all(app.tasks, { tier: "remote" });
          return [...seen.map((row) => row.id), ...unreadableRows(seen).map((row) => row.id)];
        },
        { timeout: 30_000 },
      )
      .toContain(afterId);
    expect(seen.map((row) => row.id)).not.toContain(afterId);
    expect(unreadableRows(seen)).toEqual([
      expect.objectContaining({ table: "tasks", id: afterId, column: "title" }),
    ]);

    // The large title's ciphertext reached the bucket. Nothing there, nor in
    // either object's SQLite, holds a title in the clear.
    const secrets = [small, `large title ${tag}`, afterRemoval];
    // Miniflare types its R2 binding with the Workers types, which these
    // Node tests don't load: the two calls used here.
    const bucket = (await core.miniflare.getR2Bucket("JAZZ_BLOBS")) as unknown as {
      list(): Promise<{ objects: Array<{ key: string }> }>;
      get(key: string): Promise<{ arrayBuffer(): Promise<ArrayBuffer> } | null>;
    };
    const blobs = async () => (await bucket.list()).objects;
    await expect.poll(async () => (await blobs()).length, { timeout: 30_000 }).toBeGreaterThan(0);
    for (const { key } of await blobs()) {
      const body = Buffer.from(await (await bucket.get(key))!.arrayBuffer());
      expect(leaked(body, secrets), `R2 ${key}`).toEqual([]);
    }
    await alice.session.close();
    await bob.session.close();
    await core.dispose();
    disposed = true;
    const stored = await files(persist);
    expect(stored.some((file) => file.endsWith(".sqlite"))).toBe(true);
    for (const file of stored) {
      expect(leaked(await readFile(file), secrets), file).toEqual([]);
    }
  }, 180_000);
});
