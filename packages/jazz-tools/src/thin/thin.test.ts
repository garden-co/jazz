import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb as createLocalFirstDb } from "../runtime/default-create-db.js";
import type { Db } from "../runtime/db.js";
import { formatAuthSecret } from "../runtime/auth-secret-codec.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer, type LocalJazzServerHandle } from "../testing/index.js";
import { createAccountManager, createDb, upgradeToLocalFirst } from "./index.js";
import { LOCAL_FIRST_ISSUER, mintSelfSignedToken } from "./self-signed-token.js";

const app = s.defineApp({
  notes: s.table({ text: s.string(), done: s.boolean() }, {}),
});
const permissions = definePermissions(app, ({ policy, session }) => {
  policy.notes.allowRead.where({ $createdBy: session.user });
  policy.notes.allowInsert.always();
  policy.notes.allowUpdate.where({ $createdBy: session.user });
  policy.notes.allowDelete.where({ $createdBy: session.user });
});

const opened: Db[] = [];
let server: LocalJazzServerHandle | undefined;

afterEach(async () => {
  for (const db of opened.splice(0)) await db.shutdown();
  await server?.stop();
  server = undefined;
});

async function startServer(): Promise<LocalJazzServerHandle> {
  server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  await deploy({
    serverUrl: server.url,
    appId: server.appId,
    adminSecret: server.adminSecret,
    schema: app,
    permissions,
  });
  return server;
}

async function thinDb(server: LocalJazzServerHandle, secret?: string): Promise<Db> {
  let stored: string | null = null;
  const accounts = await createAccountManager({
    appId: server.appId,
    serverUrl: server.url,
    store: {
      async read() {
        return stored;
      },
      async update(transform) {
        stored = transform(stored);
      },
    },
  });
  const db = await createDb({
    appId: server.appId,
    serverUrl: server.url,
    account: secret ? accounts.restoreLocalFirst(secret) : accounts.createLocalFirst(),
  });
  opened.push(db);
  return db;
}

describe("self-signed tokens", () => {
  it("are the tokens the native binding mints for the same seed", async () => {
    const { mintLocalFirstToken } = await import("jazz-napi");
    const seed = Buffer.alloc(32, 7).toString("base64url");
    for (const audience of ["my-app", "01234567-89ab-cdef-0123-456789abcdef"]) {
      for (let attempt = 0; ; attempt++) {
        const native = mintLocalFirstToken(seed, audience, 3600);
        const thin = mintSelfSignedToken(LOCAL_FIRST_ISSUER, {
          secret: seed,
          audience,
          ttlSeconds: 3600,
          nowSeconds: BigInt(Math.floor(Date.now() / 1000)),
        });
        // The two clocks may straddle a second.
        if (native !== thin && attempt < 3) continue;
        expect(thin).toBe(native);
        break;
      }
    }
  });
});

describe("thin client", () => {
  it("reads, writes and subscribes through the server under its own policies", async () => {
    const server = await startServer();
    const alice = await thinDb(server);
    const deliveries: string[][] = [];
    const unsubscribe = alice.subscribe(app.notes, (rows) =>
      deliveries.push(rows.map((row) => row.text).sort()),
    );
    await expect.poll(() => deliveries.length, { timeout: 10_000 }).toBeGreaterThan(0);
    expect(deliveries.at(-1)).toEqual([]);

    const written = alice.insert(app.notes, { text: "from thin", done: false });
    expect(written.value).toMatchObject({ text: "from thin", done: false });
    await written.wait({ tier: "global" });
    await expect.poll(() => deliveries.at(-1), { timeout: 10_000 }).toEqual(["from thin"]);
    expect((await alice.all(app.notes)).map((row) => row.text)).toEqual(["from thin"]);

    await alice.update(app.notes, written.value.id, { done: true }).wait({ tier: "global" });
    expect(await alice.one(app.notes.where({ id: written.value.id }))).toMatchObject({
      done: true,
    });

    // Another user sees nothing of it, through the same server-side policies.
    const bob = await thinDb(server);
    expect(await bob.all(app.notes)).toEqual([]);

    await alice.delete(app.notes, written.value.id).wait({ tier: "global" });
    await expect.poll(() => deliveries.at(-1), { timeout: 10_000 }).toEqual([]);
    unsubscribe();
  }, 60_000);

  it("shares data with a local-first client of the same account", async () => {
    const server = await startServer();
    const secret = formatAuthSecret(new Uint8Array(32).fill(9));
    const replica = await createLocalFirstDb(
      await localAccountConfig(server.appId, server.url, secret),
    );
    opened.push(replica);
    await replica.insert(app.notes, { text: "from the replica", done: false }).wait({
      tier: "global",
    });

    const thin = await thinDb(server, secret);
    expect((await thin.all(app.notes)).map((row) => row.text)).toEqual(["from the replica"]);
    await thin.insert(app.notes, { text: "from thin", done: false }).wait({ tier: "global" });
    await expect
      .poll(
        async () =>
          (await replica.all(app.notes, { tier: "remote" })).map((row) => row.text).sort(),
        { timeout: 10_000 },
      )
      .toEqual(["from the replica", "from thin"]);
  }, 60_000);

  it("upgrades to local-first with the app's live queries already loaded", async () => {
    const server = await startServer();
    const secret = formatAuthSecret(new Uint8Array(32).fill(5));
    const thin = await thinDb(server, secret);
    const unsubscribe = thin.subscribe(app.notes, () => undefined);
    await thin.insert(app.notes, { text: "before the upgrade", done: false }).wait({
      tier: "global",
    });
    thin.insert(app.notes, { text: "while upgrading", done: false });

    const local = await upgradeToLocalFirst(thin, async () =>
      createLocalFirstDb(await localAccountConfig(server.appId, server.url, secret)),
    );
    opened.push(local);
    unsubscribe();

    // The first answer comes from the local store, without asking the server.
    const first = await new Promise<string[]>((resolve) => {
      const stop = local.subscribe(
        app.notes,
        (rows) => {
          stop();
          resolve(rows.map((row) => row.text).sort());
        },
        { tier: "local-first" },
      );
    });
    expect(first).toContain("before the upgrade");
    // The thin Db finished its writes before it shut down.
    expect((await local.all(app.notes, { tier: "remote" })).map((row) => row.text).sort()).toEqual([
      "before the upgrade",
      "while upgrading",
    ]);
    await expect(thin.all(app.notes)).rejects.toThrow(/shutting down or closed/);
  }, 60_000);

  it("says plainly what it does not support yet", async () => {
    const server = await startServer();
    const db = await thinDb(server);
    await expect(
      db.transaction((tx) => {
        tx.insert(app.notes, { text: "in a transaction", done: false });
      }),
    ).rejects.toThrow(/does not support transactions/);
  }, 60_000);
});
