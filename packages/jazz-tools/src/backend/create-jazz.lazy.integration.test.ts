import { randomUUID } from "node:crypto";
import { mkdtemp, readdir, rm } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { Db } from "../runtime/db.js";
import { earlyDb, earlyDbMethodKind, MAX_EARLY_WRITES } from "./early-db.js";
import { createJazz, memory, sqlite, type Jazz } from "./index.js";

// `createJazz` at a module's top level: it opens nothing until first use,
// and a later call for the same store (a hot reload) reuses its core.

const app = s.defineApp(
  { todos: s.table({ title: s.string(), owner: s.uuid() }, {}) },
  { migrations: "none" },
);
const permissions = s.definePermissions(app, ({ policy, session }) => {
  policy.todos.allowRead.where({ owner: session.user.account });
  policy.todos.allowInsert.where({ owner: session.user.account });
  policy.todos.allowUpdate.never();
  policy.todos.allowDelete.never();
});

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  vi.restoreAllMocks();
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
});

/** Move the clock `ms` on, for a backoff to pass. */
function later(ms: number): void {
  const now = Date.now.bind(Date);
  vi.spyOn(Date, "now").mockImplementation(() => now() + ms);
}

async function tempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "jazz-lazy-"));
  cleanups.push(() => rm(dir, { recursive: true, force: true }));
  return join(dir, "store");
}

const as = (userId: string) => (_request: Request) => ({ userId });

describe("createJazz, opened on first use", () => {
  it("opens nothing when called, and its db takes writes before the store opens", async () => {
    const path = await tempDir();
    const jazz = createJazz({ app, permissions, store: sqlite({ path }) });
    cleanups.push(() => jazz.close());
    // Nothing on disk, and no app id yet.
    await expect(readdir(path)).rejects.toThrow(/ENOENT/);
    expect(() => jazz.appId).toThrow(/await jazz.ready\(\)/);
    expect(jazz.basePath).toBe("/jazz");

    // A write returns its row at once and applies as the store opens.
    const owner = randomUUID();
    const first = jazz.db.insert(app.todos, { title: "early", owner });
    expect(first.value).toMatchObject({ title: "early", owner });
    expect(first.value.id).toMatch(/^[0-9a-f-]{36}$/);
    const second = jazz.db.insert(app.todos, { title: "next", owner }, { id: randomUUID() });
    // Reads wait for the open, and see the writes made before it.
    const rows = await jazz.db.all(app.todos.where({ owner }));
    expect(rows.map((todo) => todo.title).sort()).toEqual(["early", "next"]);
    expect(rows.find((todo) => todo.title === "next")?.id).toBe(second.value.id);
    await expect(first.wait({ tier: "global" })).resolves.toEqual(first.value);
    expect(jazz.appId).toMatch(/^[0-9a-f-]{36}$/);
    expect(await readdir(path)).toContain("app-id");

    // Once open, the db is the backend's own.
    const after = jazz.db.insert(app.todos, { title: "after", owner });
    await after.wait({ tier: "global" });
    expect(await jazz.db.one(app.todos.where({ id: after.value.id }))).toMatchObject({
      title: "after",
    });
  }, 60_000);

  it("queues updates, deletes and subscriptions made before the store opens", async () => {
    const jazz = createJazz({ app, permissions, store: memory() });
    cleanups.push(() => jazz.close());
    const owner = randomUUID();
    const seen: string[][] = [];
    const stop = jazz.db.subscribe(app.todos.where({ owner }), (rows) =>
      seen.push(rows.map((todo) => todo.title)),
    );
    const { value } = jazz.db.insert(app.todos, { title: "draft", owner });
    const updated = jazz.db.update(app.todos, value.id, { title: "final" });
    // Calls that answer at once can't before the store opens.
    expect(() => jazz.db.getSyncStatus()).toThrow(/not available until the store has opened/);
    await updated.wait({ tier: "global" });
    expect(await jazz.db.one(app.todos.where({ id: value.id }))).toMatchObject({ title: "final" });
    await expect.poll(() => seen.at(-1)).toEqual(["final"]);
    stop();

    const removed = jazz.db.delete(app.todos, value.id);
    await removed.wait({ tier: "global" });
    expect(await jazz.db.all(app.todos.where({ owner }))).toEqual([]);
  }, 60_000);

  it("knows how to treat every Db method before the store opens", () => {
    const names = Object.getOwnPropertyNames(Db.prototype).filter(
      (name) => name !== "constructor" && typeof Reflect.get(Db.prototype, name) === "function",
    );
    const kinds = Object.fromEntries(names.map((name) => [name, earlyDbMethodKind(name)]));
    // A new async Db method belongs in WAITS (early-db.ts), a new write in WRITES.
    for (const name of ["insert", "restore", "update", "upsert", "delete"]) {
      expect(kinds[name]).toBe("write");
    }
    for (const name of ["all", "one", "transaction", "waitForSync", "canInsert"]) {
      expect(kinds[name]).toBe("waits");
    }
    // Internal to Db and its own callers, never reached through the early db.
    const internal = new Set([
      "inspectorAttachmentOptions",
      "allFromInspectorAttachment",
      "prefetchRuntimeQueries",
      "ensureReady",
      "allInternal",
      "runShutdown",
      "waitForOwnSync",
    ]);
    const asyncOnes = names.filter(
      (name) =>
        Reflect.get(Db.prototype, name)?.constructor?.name === "AsyncFunction" &&
        kinds[name] !== "waits" &&
        !internal.has(name),
    );
    expect(asyncOnes).toEqual([]);
  });

  it("opens one core per store and process, and a later call swaps in its options", async () => {
    const path = await tempDir();
    const request = new Request("http://app.test/api");
    const first = createJazz({
      app,
      permissions,
      store: sqlite({ path }),
      authenticate: as("alice"),
    });
    cleanups.push(() => first.close());
    const alice = await first.createAccountFor("alice");
    await expect(first.accountFor(request)).resolves.toBe(alice);
    const appId = first.appId;

    // A hot reload evaluates the module again: the same store, new options.
    const reloaded = createJazz({
      app,
      permissions,
      store: sqlite({ path }),
      authenticate: as("bob"),
      basePath: "/api/jazz",
    });
    expect(reloaded).toBe(first);
    expect(reloaded.basePath).toBe("/api/jazz");
    const bob = await reloaded.createAccountFor("bob");
    await expect(reloaded.accountFor(request)).resolves.toBe(bob);
    expect(bob).not.toBe(alice);
    expect(reloaded.appId).toBe(appId);
    // The backend's own db moved to the new session and still works.
    const owner = randomUUID();
    await reloaded.db.insert(app.todos, { title: "after reload", owner }).wait({ tier: "global" });
    expect(await reloaded.db.all(app.todos.where({ owner }))).toHaveLength(1);

    // New permissions apply to the next use: the user may no longer insert.
    const noInserts = s.definePermissions(app, ({ policy, session }) => {
      policy.todos.allowRead.where({ owner: session.user.account });
      policy.todos.allowInsert.never();
      policy.todos.allowUpdate.never();
      policy.todos.allowDelete.never();
    });
    const asBob = (store: ReturnType<typeof sqlite>, rules: typeof permissions) =>
      createJazz({ app, permissions: rules, store, authenticate: as("bob") });
    const insertAsBob = async (jazz: Jazz) =>
      (await jazz.forReqUser(request))
        .insert(app.todos, { title: "bob's", owner: bob! })
        .wait({ tier: "global" });
    await expect(insertAsBob(asBob(sqlite({ path }), permissions))).resolves.toBeTruthy();
    await expect(insertAsBob(asBob(sqlite({ path }), noInserts))).rejects.toThrow();
    await expect(insertAsBob(asBob(sqlite({ path }), permissions))).resolves.toBeTruthy();

    // Settings that would need another core are refused while it is open.
    expect(() =>
      createJazz({ app, permissions, store: sqlite({ path, blobs: "sqlite" }) }),
    ).toThrow(/already open in this process with other store settings/);

    // Closed, the next call opens the store again.
    await first.close();
    await expect(first.ready()).rejects.toThrow(/was closed/);
    const again = createJazz({ app, permissions, store: sqlite({ path }) });
    cleanups.push(() => again.close());
    expect(again).not.toBe(first);
    expect((await again.ready()).appId).toBe(appId);
  }, 60_000);

  it("applies the writes made before a close, even one made while the store opens", async () => {
    const path = await tempDir();
    const jazz = createJazz({ app, permissions, store: sqlite({ path }) });
    const owner = randomUUID();
    // Never waited for, and the close comes while the store is still opening.
    const write = jazz.db.insert(app.todos, { title: "kept", owner });
    const closing = jazz.close();
    // From the close on, the db refuses at once.
    expect(() => jazz.db.insert(app.todos, { title: "late", owner })).toThrow(/was closed/);
    await closing;
    await expect(jazz.ready()).rejects.toThrow(/was closed/);
    // The close applied it to the backend's db before closing the store,
    // rather than dropping it, and waited for it to reach the core.
    await expect(write.txId).resolves.toBeTruthy();
    // Reopened, the store has it.
    const again = createJazz({ app, permissions, store: sqlite({ path }) });
    cleanups.push(() => again.close());
    expect((await again.db.all(app.todos.where({ owner }))).map((todo) => todo.title)).toEqual([
      "kept",
    ]);
  }, 60_000);

  it("opens the store once for concurrent first uses, and once more after a close", async () => {
    const path = await tempDir();
    const request = new Request("http://app.test/api");
    const jazz = createJazz({ app, permissions, store: sqlite({ path }), authenticate: as("a") });
    const owner = randomUUID();
    // Two cores on one directory would fail on its lock: these share one.
    const [ready, rows, account, other] = await Promise.all([
      jazz.ready(),
      jazz.db.all(app.todos.where({ owner })),
      jazz.createAccountFor("a"),
      createJazz({ app, permissions, store: sqlite({ path }), authenticate: as("a") }).ready(),
    ]);
    expect(other).toBe(ready);
    expect(rows).toEqual([]);
    expect(account).toBeTruthy();
    // A createJazz while the close runs waits for it, not for a held lock.
    const closing = jazz.close();
    const next = createJazz({ app, permissions, store: sqlite({ path }) });
    cleanups.push(() => next.close());
    expect(next).not.toBe(jazz);
    const [opened] = await Promise.all([next.ready(), closing]);
    expect(opened.appId).toBe(ready.appId);
  }, 60_000);

  it("keeps the backend's session across a reload, and lets writes in flight finish", async () => {
    const path = await tempDir();
    const request = new Request("http://app.test/api");
    const rules = () =>
      s.definePermissions(app, ({ policy, session }) => {
        policy.todos.allowRead.where({ owner: session.user.account });
        policy.todos.allowInsert.where({ owner: session.user.account });
      });
    const jazz = createJazz({
      app,
      permissions: rules(),
      store: sqlite({ path }),
      authenticate: as("a"),
    });
    cleanups.push(() => jazz.close());
    const owner = randomUUID();
    const user = await jazz.createAccountFor("a");
    const userDb = await jazz.forReqUser(request);
    const inFlight = jazz.db.insert(app.todos, { title: "in flight", owner });

    // A hot reload rebuilds equal permissions: the same bundle, the same session.
    createJazz({ app, permissions: rules(), store: sqlite({ path }), authenticate: as("a") });
    await jazz.ready();
    await inFlight.wait({ tier: "global" });
    await userDb.insert(app.todos, { title: "same bundle", owner: user }).wait({ tier: "global" });

    // Changed permissions publish another bundle: what the old session had
    // in flight, and a db a request still holds, still finish.
    const pending = jazz.db.insert(app.todos, { title: "across bundles", owner });
    const changed = s.definePermissions(app, ({ policy, session }) => {
      policy.todos.allowRead.where({ owner: session.user.account });
      policy.todos.allowInsert.where({ owner: session.user.account });
      policy.todos.allowUpdate.where({ owner: session.user.account });
    });
    createJazz({ app, permissions: changed, store: sqlite({ path }), authenticate: as("a") });
    await jazz.ready();
    await pending.wait({ tier: "global" });
    await userDb.insert(app.todos, { title: "old bundle", owner: user }).wait({ tier: "global" });
    expect(
      (await jazz.db.all(app.todos.where({ owner }))).map((todo) => todo.title).sort(),
    ).toEqual(["across bundles", "in flight"]);
  }, 60_000);

  it("holds a bounded number of writes before the store opens", () => {
    const early = earlyDb(
      () => new Promise<never>(() => {}),
      () => undefined,
    );
    for (let i = 0; i < MAX_EARLY_WRITES; i++) {
      early.db.insert(app.todos, { title: `${i}`, owner: randomUUID() });
    }
    expect(() => early.db.insert(app.todos, { title: "one more", owner: randomUUID() })).toThrow(
      /writes are already waiting for the store to open/,
    );
    early.abandon(new Error("test over"));
  });

  it("keeps memory stores apart, and retries an open that failed after a backoff", async () => {
    const one = createJazz({ app, permissions, store: memory() });
    const two = createJazz({ app, permissions, store: memory() });
    cleanups.push(
      () => one.close(),
      () => two.close(),
    );
    expect(one).not.toBe(two);
    expect((await one.ready()).appId).not.toBe((await two.ready()).appId);

    const path = await tempDir();
    const previous = process.env.VERCEL;
    process.env.VERCEL = "1";
    const jazz = createJazz({ app, permissions, store: sqlite({ path }) });
    cleanups.push(() => jazz.close());
    try {
      await expect(jazz.ready()).rejects.toThrow(/or remote\(/);
      // A write made meanwhile fails with the open's error, not unhandled.
      const write = jazz.db.insert(app.todos, { title: "lost", owner: randomUUID() });
      await expect(write.wait({ tier: "local" })).rejects.toThrow(/or remote\(/);
    } finally {
      if (previous === undefined) delete process.env.VERCEL;
      else process.env.VERCEL = previous;
    }
    // Not at once: until its backoff passes, a use fails with the same error.
    await expect(jazz.ready()).rejects.toThrow(/or remote\(/);
    later(2_000);
    await jazz.ready();
  }, 60_000);

  it("serves requests before it has opened, and leaves other routes alone", async () => {
    const path = await tempDir();
    const jazz: Jazz = createJazz({ app, permissions, store: sqlite({ path }) });
    cleanups.push(() => jazz.close());
    const server: Server = createServer((req, res) =>
      jazz.nodeHandler(req, res, () => {
        res.statusCode = 404;
        res.end("app route");
      }),
    );
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    cleanups.push(() => new Promise((resolve) => server.close(resolve)));
    const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;

    // An app route passes on without opening the store.
    const other = await fetch(`${origin}/elsewhere`);
    expect(await other.text()).toBe("app route");
    await expect(readdir(path)).rejects.toThrow(/ENOENT/);

    // A Jazz route opens it and is answered.
    const answered = await fetch(`${origin}/jazz/apps/any/schemas`);
    expect(await answered.text()).not.toBe("app route");
    expect(jazz.appId).toMatch(/^[0-9a-f-]{36}$/);
    expect(await readdir(path)).toContain("app-id");
  }, 60_000);
});
