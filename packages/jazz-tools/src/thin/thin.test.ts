import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb as createLocalFirstDb } from "../runtime/default-create-db.js";
import type { Db } from "../runtime/db.js";
import { formatAuthSecret } from "../runtime/auth-secret-codec.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { startTestJazz, type TestJazz } from "../testing/index.js";
import { createAccountManager, createClient, upgradeToLocalFirst } from "./index.js";
import { ThinRuntime } from "./thin-runtime.js";
import { decodeThinResponse, isThinResponse } from "./wire.js";
import { JazzClient } from "../runtime/client.js";
import type { Session } from "../runtime/context.js";
import { LOCAL_FIRST_ISSUER, mintSelfSignedToken } from "./self-signed-token.js";
import { parseSnapshotRef, serializeSnapshotRef } from "../runtime/snapshot-ref.js";

const app = s.defineApp(
  {
    notes: s.table({ text: s.string(), done: s.boolean() }, {}),
    documents: s.table({ branch: s.string(), title: s.string() }, {}).branchBy("branch"),
    files: s.table({ name: s.string(), data: s.bytes() }, {}),
  },
  { migrations: "none" },
);
const permissions = definePermissions(app, ({ policy, session }) => {
  policy.notes.allowRead.where({ $createdBy: session.user });
  policy.notes.allowInsert.always();
  policy.notes.allowUpdate.where({ $createdBy: session.user });
  policy.notes.allowDelete.where({ $createdBy: session.user });
  policy.documents.allowRead.where({ $createdBy: session.user });
  policy.documents.allowInsert.always();
  policy.documents.allowUpdate.where({ $createdBy: session.user });
  policy.files.allowRead.where({ $createdBy: session.user });
  policy.files.allowInsert.always();
});

const opened: Db[] = [];
let server: TestJazz | undefined;

afterEach(async () => {
  for (const db of opened.splice(0)) await db.shutdown();
  await server?.stop();
  server = undefined;
});

async function startServer(): Promise<TestJazz> {
  server = await startTestJazz({
    app: app,
    permissions: permissions,
    coreOptions: { allowLocalFirstAuth: true, inMemory: true },
  });
  return server;
}

async function thinDb(server: TestJazz, secret?: string): Promise<Db> {
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
  const db = await createClient({
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

  it("writes and reads values over a wire frame, and refuses writes past the most one takes", async () => {
    const server = await startServer();
    const alice = await thinDb(server);
    const deliveries: number[][] = [];
    const unsubscribe = alice.subscribe(app.notes, (rows) =>
      deliveries.push(rows.map((row) => row.text.length)),
    );
    await expect.poll(() => deliveries.length, { timeout: 10_000 }).toBeGreaterThan(0);
    // A 3 MB value goes out in parts, and comes back in parts, both as a
    // query's answer and as a live query's update.
    const long = "t".repeat(3 * 1024 * 1024);
    const written = alice.insert(app.notes, { text: long, done: false });
    await written.wait({ tier: "global" });
    expect((await alice.all(app.notes)).map((row) => row.text)).toEqual([long]);
    await expect.poll(() => deliveries.at(-1), { timeout: 10_000 }).toEqual([long.length]);
    // Past the most one request takes, refused here, and its wait settles.
    const huge = alice.insert(app.notes, { text: "h".repeat(33 * 1024 * 1024), done: false });
    await expect(huge.wait({ tier: "global" })).rejects.toThrow("sends in one request");
    // The connection goes on.
    await alice.update(app.notes, written.value.id, { done: true }).wait({ tier: "global" });
    unsubscribe();
  }, 120_000);

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

  it("commits a transaction's writes as one and discards a rolled-back one", async () => {
    const server = await startServer();
    const db = await thinDb(server);
    const committed = await db.transaction((tx) => {
      tx.insert(app.notes, { text: "first", done: false });
      tx.insert(app.notes, { text: "second", done: false });
      return "done";
    });
    expect(committed.value).toBe("done");
    await committed.wait({ tier: "global" });
    expect((await db.all(app.notes)).map((row) => row.text).sort()).toEqual(["first", "second"]);

    const discarded = db.beginTransaction();
    discarded.insert(app.notes, { text: "discarded", done: false });
    discarded.rollback();
    const serializable = await db.serializableTransaction((tx) => {
      tx.insert(app.notes, { text: "serializable", done: true });
    });
    await serializable.wait();
    expect((await db.all(app.notes)).map((row) => row.text).sort()).toEqual([
      "first",
      "second",
      "serializable",
    ]);
  }, 60_000);

  it("reads its own staged writes inside a transaction", async () => {
    const server = await startServer();
    const db = await thinDb(server);
    const tx = db.beginTransaction();
    tx.insert(app.notes, { text: "staged", done: false });
    expect((await tx.all(app.notes)).map((row) => row.text)).toEqual(["staged"]);
    expect(await db.all(app.notes)).toEqual([]);
    await tx.commit().wait({ tier: "global" });
    expect((await db.all(app.notes)).map((row) => row.text)).toEqual(["staged"]);
  }, 60_000);

  it("writes and reads through branch views and frozen snapshots", async () => {
    const server = await startServer();
    const db = await thinDb(server);
    const original = db.insert(
      app.documents,
      { branch: "main", title: "Original" },
      { branch: "main" },
    );
    await original.wait({ tier: "global" });
    const snapshot = parseSnapshotRef(serializeSnapshotRef(await db.captureSnapshot()));
    await db
      .update(app.documents, original.value.id, { title: "Changed" }, { branch: "main" })
      .wait({ tier: "global" });

    const frozen = { branch: "draft", base: ["main", snapshot] } as const;
    expect(await db.one(app.documents, frozen)).toMatchObject({
      id: original.value.id,
      branch: "draft",
      title: "Original",
    });
    expect(await db.one(app.documents, { branch: "draft", base: "main" })).toMatchObject({
      title: "Changed",
    });
    await db
      .update(app.documents, original.value.id, { title: "Draft" }, frozen)
      .wait({ tier: "global" });
    expect(await db.one(app.documents, frozen)).toMatchObject({ title: "Draft" });
    expect(await db.one(app.documents, { branch: "main" })).toMatchObject({ title: "Changed" });
  }, 60_000);

  it("keeps each user's live queries to that user's rows", async () => {
    const server = await startServer();
    const alice = await thinDb(server);
    const bob = await thinDb(server);
    const seen = { alice: [] as string[][], bob: [] as string[][] };
    const stopAlice = alice.subscribe(app.notes, (rows) =>
      seen.alice.push(rows.map((row) => row.text).sort()),
    );
    const stopBob = bob.subscribe(app.notes, (rows) =>
      seen.bob.push(rows.map((row) => row.text).sort()),
    );
    await bob.insert(app.notes, { text: "bob's", done: false }).wait({ tier: "global" });
    await alice.insert(app.notes, { text: "alice's", done: false }).wait({ tier: "global" });
    await expect.poll(() => seen.alice.at(-1), { timeout: 10_000 }).toEqual(["alice's"]);
    await expect.poll(() => seen.bob.at(-1), { timeout: 10_000 }).toEqual(["bob's"]);
    expect(seen.alice.flat()).not.toContain("bob's");
    expect(seen.bob.flat()).not.toContain("alice's");
    stopAlice();
    stopBob();
  }, 60_000);

  it("reopens live queries after a reconnect, replacing what they delivered", async () => {
    const server = await startServer();
    const db = await thinDb(server);
    const deliveries: string[][] = [];
    const stop = db.subscribe(app.notes, (rows) => deliveries.push(rows.map((row) => row.text)));
    await db.insert(app.notes, { text: "before", done: false }).wait({ tier: "global" });
    await expect.poll(() => deliveries.at(-1), { timeout: 10_000 }).toEqual(["before"]);

    reconnect(db);
    await db.insert(app.notes, { text: "after", done: false }).wait({ tier: "global" });
    // The reopened query's first answer replaces the rows, not adds to them.
    await expect
      .poll(() => deliveries.at(-1)?.slice().sort(), { timeout: 10_000 })
      .toEqual(["after", "before"]);
    expect(deliveries.every((rows) => new Set(rows).size === rows.length)).toBe(true);
    stop();
  }, 60_000);

  it("sends a write again when its answer is lost with the connection, and the server writes it once", async () => {
    const server = await startServer();
    const db = await thinDb(server);
    await db.insert(app.notes, { text: "first", done: false }).wait({ tier: "global" });
    const runtime = db.currentRuntime() as ThinRuntime;
    // The server commits each write, but the first answer to each never
    // arrives: its connection drops as it comes in.
    const internals = runtime as unknown as {
      receive(carrier: unknown, piece: Uint8Array): void;
      lost(carrier: unknown, error: { code: string; retry: string; message: string }): void;
    };
    const receive = internals.receive.bind(runtime);
    let drops = 2;
    internals.receive = (carrier, piece) => {
      if (drops > 0 && isThinResponse(piece) && decodeThinResponse(piece).body.type === "written") {
        drops -= 1;
        internals.lost(carrier, {
          code: "websocket_closed",
          retry: "later",
          message: "the answer was lost",
        });
        return;
      }
      receive(carrier, piece);
    };

    const inserted = db.insert(app.notes, { text: "in flight", done: false });
    await inserted.wait({ tier: "global" });
    await db.update(app.notes, inserted.value.id, { done: true }).wait({ tier: "global" });
    expect(drops).toBe(0);

    const rows = await db.all(app.notes);
    expect(rows.map((row) => [row.text, row.done]).sort()).toEqual([
      ["first", false],
      ["in flight", true],
    ]);
    expect(runtime.pendingUploadCount()).toBe(0);
  }, 60_000);

  it("does not send a write again under a new credential, which may name another user", async () => {
    const server = await startServer();
    const db = await thinDb(server);
    await db.insert(app.notes, { text: "first", done: false }).wait({ tier: "global" });
    const runtime = db.currentRuntime() as ThinRuntime;
    // The write's answer is lost with its connection; before the client
    // reconnects, the app hands it a new credential.
    const internals = runtime as unknown as {
      receive(carrier: unknown, piece: Uint8Array): void;
      lost(carrier: unknown, error: { code: string; retry: string; message: string }): void;
    };
    const receive = internals.receive.bind(runtime);
    let dropped = false;
    internals.receive = (carrier, piece) => {
      if (!dropped && isThinResponse(piece) && decodeThinResponse(piece).body.type === "written") {
        dropped = true;
        internals.lost(carrier, {
          code: "websocket_closed",
          retry: "later",
          message: "the answer was lost",
        });
        reconnect(db);
        return;
      }
      receive(carrier, piece);
    };

    const write = db.insert(app.notes, { text: "in flight", done: false });
    // It may have committed as the old user, so it ends with its fate unknown.
    await expect(write.wait({ tier: "global" })).rejects.toThrow();
    expect(dropped).toBe(true);
    expect(runtime.pendingUploadCount()).toBe(0);
    // The connection the new credential opened serves the next write.
    await db.insert(app.notes, { text: "after", done: false }).wait({ tier: "global" });
    expect((await db.all(app.notes)).map((row) => row.text).sort()).toEqual([
      "after",
      "first",
      "in flight",
    ]);
  }, 60_000);

  it("does not send a write again when connect() names another credential", async () => {
    const server = await startServer();
    const db = await thinDb(server);
    await db.insert(app.notes, { text: "first", done: false }).wait({ tier: "global" });
    const runtime = db.currentRuntime() as ThinRuntime;
    const internals = runtime as unknown as {
      url: string;
      authJson: string;
      receive(carrier: unknown, piece: Uint8Array): void;
      lost(carrier: unknown, error: { code: string; retry: string; message: string }): void;
    };
    const receive = internals.receive.bind(runtime);
    let dropped = false;
    internals.receive = (carrier, piece) => {
      if (!dropped && isThinResponse(piece) && decodeThinResponse(piece).body.type === "written") {
        dropped = true;
        internals.lost(carrier, {
          code: "websocket_closed",
          retry: "later",
          message: "the answer was lost",
        });
        // Before it reconnects, it is connected again with a credential of
        // its own (here the same one, spelled differently).
        runtime.connect(internals.url, JSON.stringify(JSON.parse(internals.authJson), null, 1));
        return;
      }
      receive(carrier, piece);
    };

    const write = db.insert(app.notes, { text: "in flight", done: false });
    await expect(write.wait({ tier: "global" })).rejects.toThrow();
    expect(dropped).toBe(true);
    expect(runtime.pendingUploadCount()).toBe(0);
    await db.insert(app.notes, { text: "after", done: false }).wait({ tier: "global" });
    expect((await db.all(app.notes)).map((row) => row.text).sort()).toEqual([
      "after",
      "first",
      "in flight",
    ]);
  }, 60_000);

  it("gives up a write in flight when the app goes offline, so waiting for sync ends", async () => {
    const server = await startServer();
    const db = await thinDb(server);
    await db.insert(app.notes, { text: "first", done: false }).wait({ tier: "global" });
    const runtime = db.currentRuntime() as ThinRuntime;

    // Sent, not answered yet: the app's offline toggle cuts the connection.
    const write = db.insert(app.notes, { text: "in flight", done: false });
    await db.disconnect();

    // It may have committed, so it is not sent again: its fate is unknown.
    await expect(write.wait({ tier: "global" })).rejects.toThrow();
    expect(runtime.pendingUploadCount()).toBe(0);
    await db.waitForSync({ timeoutMs: 1_000 });
  }, 60_000);

  it("waits for sync past a rejected write the app already had", async () => {
    const server = await startServer();
    const db = await thinDb(server);
    const other = await thinDb(server);
    const theirs = await other
      .insert(app.notes, { text: "theirs", done: false })
      .wait({ tier: "global" });
    // Only the row's creator may update it: the server rejects this.
    await expect(
      db.update(app.notes, theirs.id, { done: true }).wait({ tier: "global" }),
    ).rejects.toThrow();
    await expect(db.waitForSync({ timeoutMs: 5_000 })).resolves.toBeUndefined();

    await db.insert(app.notes, { text: "mine", done: false }).wait({ tier: "global" });
    await expect(db.waitForSync({ timeoutMs: 5_000 })).resolves.toBeUndefined();
  }, 60_000);

  it("fails a transaction its connection took with it, and nothing else", async () => {
    const server = await startServer();
    const db = await thinDb(server);
    const tx = db.beginTransaction();
    tx.insert(app.notes, { text: "staged", done: false });
    // The server has the transaction open.
    expect((await tx.all(app.notes)).map((row) => row.text)).toEqual(["staged"]);

    // A credential renewal takes a new connection, which ends the old one's
    // transactions on the server.
    reconnect(db);
    expect(() => tx.insert(app.notes, { text: "more", done: false })).toThrow(
      /lost with the connection/,
    );
    await expect(tx.commit().wait({ tier: "global" })).rejects.toThrow(/lost with the connection/);

    const next = await db.transaction((again) => {
      again.insert(app.notes, { text: "again", done: false });
    });
    await next.wait({ tier: "global" });
    expect((await db.all(app.notes)).map((row) => row.text)).toEqual(["again"]);
  }, 60_000);

  it("refuses reads, writes and transactions as another session", async () => {
    const server = await startServer();
    const db = await thinDb(server);
    await db.all(app.notes);
    const runtime = db.currentRuntime() as ThinRuntime;
    const session = JSON.stringify({ issuer: "urn:test", user_id: "someone-else" });
    const query = JSON.stringify({ table: "notes" });
    await expect(runtime.query(query, session)).rejects.toThrow(/not supported in thin mode/);
    expect(() => runtime.createSubscription(query, session)).toThrow(/not supported in thin mode/);
    expect(() => runtime.beginTransaction("mergeable", "tx-other" as never, session)).toThrow(
      /not supported in thin mode/,
    );
    const asOther = JSON.stringify({ session: JSON.parse(session), transaction_id: "tx-1" });
    for (const writeContext of [session, asOther]) {
      expect(() => runtime.insert("notes", {} as never, writeContext)).toThrow(
        /not supported in thin mode/,
      );
      expect(() =>
        runtime.delete("notes", "00000000-0000-7000-8000-000000000000", writeContext),
      ).toThrow(/not supported in thin mode/);
    }
    // Its own session is served.
    await db.insert(app.notes, { text: "mine", done: false }).wait({ tier: "global" });

    // A connection that does not know its author (a cookie session)
    // refuses any session named explicitly.
    const anonymous = new ThinRuntime((runtime as unknown as { schema: never }).schema);
    await expect(anonymous.query(query, session)).rejects.toThrow(/not supported in thin mode/);
    expect(() => anonymous.insert("notes", {} as never, session)).toThrow(
      /not supported in thin mode/,
    );
  }, 60_000);

  it("serves a cookie-session client's own session once it resolves", async () => {
    const server = await startServer();
    const db = await thinDb(server);
    await db.all(app.notes);
    const schema = (db.currentRuntime() as unknown as { schema: never }).schema;
    // A cookie-session client knows no session when it connects.
    const runtime = new ThinRuntime(schema);
    const client = JazzClient.connectWithRuntime(runtime, { appId: server.appId, schema });
    const mine = { issuer: "urn:test", user_id: "cookie-user" } as Session;
    const other = JSON.stringify({ issuer: "urn:test", user_id: "someone-else" });
    const values = {} as never;
    expect(() => runtime.insert("notes", values, JSON.stringify(mine))).toThrow(
      /not supported in thin mode/,
    );
    // Its session resolves later; calls naming it are its own (and fail
    // only on these placeholder values).
    client.updateCookieSession(mine);
    expect(() => runtime.insert("notes", values, JSON.stringify(mine))).toThrow(/encoding error/);
    expect(() => runtime.insert("notes", values, other)).toThrow(/not supported in thin mode/);
    runtime.close();
  }, 60_000);

  it("streams a large value into a row", async () => {
    const server = await startServer();
    const db = await thinDb(server);
    const size = 200 * 1024;
    const expected = new Uint8Array(size).map((_, index) => index % 251);
    async function* chunks(): AsyncGenerator<Uint8Array> {
      for (let offset = 0; offset < size; offset += 50_000) {
        yield expected.subarray(offset, offset + 50_000);
      }
    }
    const written = await db.insertStreaming(app.files, { name: "blob", data: chunks() });
    await written.wait({ tier: "global" });
    const row = await db.one(app.files.where({ id: written.value.id }));
    expect(row?.name).toBe("blob");
    expect(row?.data).toEqual(expected);
  }, 60_000);

  it("streams a large value inside a transaction", async () => {
    const server = await startServer();
    const db = await thinDb(server);
    const size = 150 * 1024;
    const expected = new Uint8Array(size).map((_, index) => index % 241);
    async function* chunks(): AsyncGenerator<Uint8Array> {
      for (let offset = 0; offset < size; offset += 40_000) {
        yield expected.subarray(offset, offset + 40_000);
      }
    }
    const tx = db.beginTransaction();
    const { id } = await tx.insertStreaming(app.files, {
      name: "in a transaction",
      data: chunks(),
    });
    expect(await db.all(app.files)).toEqual([]);
    await tx.commit().wait({ tier: "global" });
    expect((await db.one(app.files.where({ id })))?.data).toEqual(expected);

    const discarded = db.beginTransaction();
    await discarded.insertStreaming(app.files, { name: "discarded", data: chunks() });
    await discarded.rollback();
    expect((await db.all(app.files)).map((row) => row.name)).toEqual(["in a transaction"]);
  }, 60_000);

  it("fails the commit when a source throws mid-stream inside a transaction", async () => {
    const server = await startServer();
    const db = await thinDb(server);
    async function* failing(): AsyncGenerator<Uint8Array> {
      yield new Uint8Array(40_000).fill(7);
      throw new Error("source broke");
    }
    const tx = db.beginTransaction();
    tx.insert(app.files, { name: "sibling", data: new Uint8Array([1]) });
    await expect(
      tx.insertStreaming(app.files, { name: "broken", data: failing() }),
    ).rejects.toThrow("source broke");
    await expect(tx.commit().wait({ tier: "global" })).rejects.toThrow();
    expect(await db.all(app.files)).toEqual([]);
  }, 60_000);
});

/** Renew the credential with itself: a thin client reconnects for a new one. */
function reconnect(db: Db): void {
  const runtime = db.currentRuntime();
  if (!(runtime instanceof ThinRuntime)) throw new Error("not a thin Db");
  runtime.updateAuth((runtime as unknown as { authJson: string }).authJson);
}
