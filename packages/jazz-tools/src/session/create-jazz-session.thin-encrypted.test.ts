import { mkdtemp, readdir, readFile } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { afterEach, describe, expect, it, vi } from "vitest";
import { acceptDeviceAccounts, createJazz, sqlite } from "../backend/index.js";
import type { AccountStore } from "../accounts/persistence.js";
import {
  EncryptionNotDeclaredError,
  memoryPrivateStore,
  type PrivateStore,
} from "../encryption/index.js";
import { createDb as createLocalFirstDb } from "../runtime/default-create-db.js";
import { Db } from "../runtime/db.js";
import { DbReplacedError } from "../runtime/db-replaced-error.js";
import { DbClosedError } from "../runtime/graceful-shutdown-error.js";
import { RuntimeClosedError } from "../runtime/runtime-closed-error.js";
import { EncryptionNeedsLocalFirstError } from "../runtime/encryption-needs-local-first.js";
import { isThinDb, type LocalFirstLoader } from "../runtime/local-first-upgrade.js";
import type { MutationErrorEvent } from "../runtime/client.js";
import { testApp } from "../testing/index.js";
import { openBrowserSessionCore, type BrowserJazzSession } from "./create-jazz-session.js";

// A browser session of an app with end-to-end encrypted columns starts on the
// thin client, which holds no keys. What touches an encrypted column waits for
// the local-first client (starting it), then runs there; plain work stays thin.
//
//   app ──insert(tasks.title)──► thin Db ──refused──► wait ──► local-first Db ──► server
//                                   │
//                                   └── insert(notes) ──► queued behind it, same order

const app = testApp({
  format: 1,
  tables: {
    projects: { columns: { name: "string", ownerId: "uuid" } },
    tasks: {
      columns: { projectId: "uuid", title: "bytes", __e2ee_eq_title: "bytes", note: "string?" },
      relations: { project: { rel: "projects", column: "projectId" } },
      encryption: {
        space: "projectId",
        scope: "projects",
        columns: { title: "string" },
        equality: ["title"],
      },
    },
    // Plain; a comment's insert is allowed only once its task is on the server.
    comments: {
      columns: { taskId: "uuid", text: "string" },
      relations: { task: { rel: "tasks", column: "taskId" } },
    },
    notes: { columns: { text: "string" } },
    // Plain; a reply's insert is allowed only once its note is on the server.
    replies: {
      columns: { noteId: "uuid", text: "string" },
      relations: { note: { rel: "notes", column: "noteId" } },
    },
  },
});
const members =
  "exists __e2ee_members(scopeId = table_id('projects'), identifier = this.id, accountId = session.user)";
const permissions = {
  $schema: "https://jazz.tools/schemas/jazz.permissions.v1.json",
  tables: {
    projects: {
      read: members,
      insert: "$createdBy.account = session.user",
      update: "ownerId = session.user",
    },
    // Anyone reads a task's row, so an account outside its space sees its
    // plain columns and holds no key for its encrypted one.
    tasks: { read: "true", insert: "allowedTo.read(project)" },
    comments: { read: "true", insert: "allowedTo.read(task)" },
    notes: { read: "true", insert: "true" },
    replies: { read: "true", insert: "allowedTo.read(note)" },
  },
} as never;
const membersTable = (app as never as Record<string, never>)["__e2ee_members"];
const tasks = app.tasks as never as {
  select(...columns: string[]): { where(where: object): never };
  where(where: object): never;
};
const texts = (rows: unknown[]) => rows.map((row) => (row as { text: string }).text);
const idOf = (row: unknown) => (row as { id: string }).id;
const titles = (rows: unknown[]) => rows.map((row) => (row as { title: string }).title);

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  vi.useRealTimers();
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

async function serve() {
  const dir = await mkdtemp(`${tmpdir()}/thin-encrypted-`);
  const jazz = await createJazz({
    app: app as never,
    permissions,
    store: sqlite({ path: `${dir}/core.sqlite` }),
    // The app's own sign-in (`Authorization: Bearer token-<user>`), and device accounts.
    authenticate: (request: Request) => {
      const match = /^Bearer token-(\w+)$/.exec(request.headers.get("authorization") ?? "");
      return match
        ? { userId: match[1]!, expiresAt: Math.floor(Date.now() / 1000) + 3600 }
        : acceptDeviceAccounts(request);
    },
  }).ready();
  cleanups.push(() => jazz.close());
  const server: Server = createServer((req, res) =>
    jazz.nodeHandler(req, res, () => {
      res.statusCode = 404;
      res.end();
    }),
  );
  await jazz.attach(server);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  cleanups.push(
    () =>
      new Promise((resolve) => {
        server.closeAllConnections();
        server.close(resolve);
      }),
  );
  return {
    jazz,
    serverUrl: `http://127.0.0.1:${(server.address() as AddressInfo).port}/jazz`,
    storePath: `${dir}/core.sqlite`,
  };
}
type Host = Awaited<ReturnType<typeof serve>>;

function memoryStore(): AccountStore {
  let stored: string | null = null;
  return {
    read: async () => stored,
    update: async (transform) => void (stored = transform(stored)),
  };
}

async function storedBytes(path: string): Promise<Buffer> {
  const entries = await readdir(path, { withFileTypes: true }).catch(() => undefined);
  if (!entries) return readFile(path);
  const parts = await Promise.all(entries.map((entry) => storedBytes(`${path}/${entry.name}`)));
  return Buffer.concat(parts);
}

const loadLocalFirst: LocalFirstLoader = async () => ({ createClient: createLocalFirstDb });

/** A loader that loads only once `release()` is called, counting its calls. */
function gatedLoader() {
  let release!: () => void;
  const released = new Promise<void>((resolve) => (release = resolve));
  const gate = {
    calls: 0,
    release: () => release(),
    loader: (async () => {
      gate.calls++;
      await released;
      return { createClient: createLocalFirstDb };
    }) as LocalFirstLoader,
  };
  return gate;
}

/** The page is still loading: the background upgrade waits for a load event that doesn't come. */
function pageStillLoading() {
  const events = { addEventListener() {}, removeEventListener() {} };
  vi.stubGlobal("document", { ...events, readyState: "loading", visibilityState: "visible" });
  vi.stubGlobal("window", { ...events, document: globalThis.document });
}

interface Device {
  accounts: AccountStore;
  privateStore: PrivateStore;
}

async function open(
  host: Host,
  device: Device = { accounts: memoryStore(), privateStore: memoryPrivateStore() },
  client?: LocalFirstLoader,
): Promise<BrowserJazzSession> {
  const session = await openBrowserSessionCore({
    // An encrypted app serves only device accounts founded for its own app id.
    scope: host.jazz.appId,
    serverUrl: host.serverUrl,
    store: device.accounts,
    initial: "device",
    driver: { type: "memory" },
    encryption: { app, store: device.privateStore },
    ...(client ? { client } : {}),
  });
  cleanups.push(() => session.close());
  return session;
}

const db = (session: BrowserJazzSession): Db => session.getSnapshot().client!.db;

/** A device of `user`, signed in by the app, with its own private store. */
async function userDevice(host: Host, user: string, client?: LocalFirstLoader) {
  const session = await openBrowserSessionCore({
    scope: host.jazz.appId,
    serverUrl: host.serverUrl,
    store: memoryStore(),
    driver: { type: "memory" },
    bearer: () => `token-${user}`,
    encryption: { app, store: memoryPrivateStore() },
    ...(client ? { client } : {}),
  } as never);
  cleanups.push(() => session.close());
  return session;
}

/**
 * A device account that is a member of a project holding one task, `kept`,
 * made on the local-first client in an earlier visit. Its next visit starts
 * thin again.
 */
async function member(host: Host) {
  const device: Device = { accounts: memoryStore(), privateStore: memoryPrivateStore() };
  const first = await open(host, device, loadLocalFirst);
  await expect.poll(() => isThinDb(db(first)), { timeout: 20_000 }).toBe(false);
  expect(await db(first).encryption.ready({ timeoutMs: 20_000 })).toBe("active");
  const account = first.getSnapshot().client!.session!.user!;
  const project = crypto.randomUUID();
  const self = await db(first).encryption.members.row({ table: "projects", id: project }, account);
  await (
    await db(first).transaction((tx) => {
      tx.insert(app.projects, { name: "plans", ownerId: account } as never, { id: project });
      tx.insert(membersTable, self);
    })
  ).wait({ tier: "global" });
  await db(first)
    .insert(app.tasks, { projectId: project, title: "kept" } as never)
    .wait({ tier: "global" });
  await first.close();
  return { device, project };
}

/** `member`'s next visit: thin, with `loader` gated until released. */
async function thinMember(host: Host) {
  const { device, project } = await member(host);
  const gate = gatedLoader();
  const session = await open(host, device, gate.loader);
  expect(isThinDb(db(session))).toBe(true);
  return { session, kept: db(session), gate, project };
}

/** What `promise` settles with: its value, or its error. */
function settled<T>(promise: Promise<T>): Promise<{ value?: T; error?: unknown }> {
  return promise.then(
    (value) => ({ value }),
    (error: unknown) => ({ error }),
  );
}

describe("a thin session of an app with encrypted columns", () => {
  it("refuses without a loader and logs once", async () => {
    const host = await serve();
    const logged = vi.spyOn(console, "error").mockImplementation(() => {});
    const session = await open(host);
    const project = crypto.randomUUID();

    const first = await settled(
      Promise.resolve().then(() =>
        db(session).insert(app.tasks, { projectId: project, title: "secret" } as never),
      ),
    );
    const second = await settled(db(session).all(app.tasks));

    for (const { error } of [first, second]) {
      expect(error).toBeInstanceOf(EncryptionNeedsLocalFirstError);
      expect((error as EncryptionNeedsLocalFirstError).reason).toBe("no-loader");
    }
    expect((first.error as Error).message).toContain(
      'Give Jazz a loader for it, `client: () => import("jazz-tools/local-first")`',
    );
    expect(logged.mock.calls.filter(([error]) => error === first.error)).toHaveLength(1);
    expect(
      logged.mock.calls.filter(([error]) => error instanceof EncryptionNeedsLocalFirstError),
    ).toHaveLength(1);
  }, 60_000);

  it("plain reads and writes of an encrypted app do not call the loader", async () => {
    const host = await serve();
    pageStillLoading();
    const gate = gatedLoader();
    const session = await open(host, undefined, gate.loader);
    const kept = db(session);

    const seen: string[][] = [];
    const stop = kept.subscribe(app.notes, (rows) => seen.push(rows.map((row) => row.text)));
    await kept.insert(app.notes, { text: "plain" } as never).wait({ tier: "global" });
    expect((await kept.all(app.notes, { tier: "remote" })).map((row) => row.text)).toEqual([
      "plain",
    ]);
    // Plain columns of the encrypted table, filtered by its space column.
    await expect(
      kept.all(tasks.select("projectId", "note").where({ projectId: crypto.randomUUID() })),
    ).resolves.toEqual([]);
    await expect.poll(() => seen.at(-1), { timeout: 10_000 }).toEqual(["plain"]);
    stop();

    expect(gate.calls).toBe(0);
    expect(isThinDb(kept)).toBe(true);
  }, 60_000);

  it("an encrypted write before page load starts the upgrade, and runs on the local-first client", async () => {
    const host = await serve();
    const { device, project } = await member(host);
    pageStillLoading();
    const gate = gatedLoader();
    const session = await open(host, device, gate.loader);
    const kept = db(session);
    expect(gate.calls).toBe(0);

    const written = kept.insert(app.tasks, { projectId: project, title: "waited" } as never);
    expect(written.value.title).toBe("waited");
    await expect.poll(() => gate.calls, { timeout: 10_000 }).toBe(1);
    gate.release();

    const row = await written.wait({ tier: "global" });
    expect(row.id).toBe(written.value.id);
    expect(isThinDb(kept)).toBe(false);
    const rows = await kept.all(tasks.where({ projectId: project }));
    expect(titles(rows).sort()).toEqual(["kept", "waited"]);
  }, 90_000);

  it("stores a waited insert's encrypted cell as ciphertext, not its plaintext", async () => {
    const host = await serve();
    const { kept, gate, project } = await thinMember(host);
    const marker = `plaintext-${crypto.randomUUID()}`;

    const written = kept.insert(app.tasks, { projectId: project, title: marker } as never);
    gate.release();
    await written.wait({ tier: "global" });
    await host.jazz.close();

    const stored = await storedBytes(host.storePath);
    expect(stored.includes(Buffer.from(marker))).toBe(false);
  }, 90_000);

  it("keeps the order of writes made while waiting", async () => {
    const host = await serve();
    const { kept, gate, project } = await thinMember(host);

    // The comment's insert is allowed only once its task is on the server.
    const task = kept.insert(app.tasks, { projectId: project, title: "first" } as never);
    const comment = kept.insert(app.comments, {
      taskId: task.value.id,
      text: "about the task",
    } as never);
    gate.release();

    await task.wait({ tier: "global" });
    await comment.wait({ tier: "global" });
    expect(isThinDb(kept)).toBe(false);
    expect((await kept.all(app.comments, { tier: "remote" })).map((row) => row.text)).toEqual([
      "about the task",
    ]);
  }, 90_000);

  it("a write made right after the hand-over runs after the queued ones", async () => {
    const host = await serve();
    const { kept, gate, project } = await thinMember(host);
    const task = kept.insert(app.tasks, { projectId: project, title: "queued" } as never);
    const taskId = task.value.id;
    // The moment the thin Db hands over, the app writes again.
    const adopt = Db.prototype.adoptSuccessor;
    let after: ReturnType<Db["insert"]> | undefined;
    vi.spyOn(Db.prototype, "adoptSuccessor").mockImplementation(function (this: Db, next: Db) {
      adopt.call(this, next);
      if (this === kept) after = kept.insert(app.comments, { taskId, text: "after" } as never);
    });
    gate.release();

    await task.wait({ tier: "global" });
    await expect.poll(() => after, { timeout: 20_000 }).toBeDefined();
    await after!.wait({ tier: "global" });
    expect((await kept.all(app.comments, { tier: "remote" })).map((row) => row.text)).toEqual([
      "after",
    ]);
  }, 90_000);

  it("a plain read answers while a write waits", async () => {
    const host = await serve();
    const gate = gatedLoader();
    const session = await open(host, undefined, gate.loader);
    const kept = db(session);
    await kept.insert(app.notes, { text: "plain" } as never).wait({ tier: "global" });

    const waiting = kept.insert(app.tasks, {
      projectId: crypto.randomUUID(),
      title: "waits",
    } as never);
    let settledWrite = false;
    void waiting
      .wait({ tier: "global" })
      .catch(() => undefined)
      .finally(() => (settledWrite = true));

    const rows = await kept.all(app.notes, { tier: "remote" });
    expect(rows.map((row) => row.text)).toEqual(["plain"]);
    expect(settledWrite).toBe(false);
    expect(isThinDb(kept)).toBe(true);
  }, 60_000);

  it("rejects waiting operations with the loader's error", async () => {
    const host = await serve();
    const warned = vi.spyOn(console, "warn").mockImplementation(() => {});
    const failure = new Error("Failed to fetch dynamically imported module");
    let fail!: (error: Error) => void;
    const loading = new Promise<never>((_, reject) => (fail = reject));
    const session = await open(host, undefined, () => loading);
    const kept = db(session);

    const write = kept.insert(app.tasks, { projectId: crypto.randomUUID(), title: "x" } as never);
    const written = settled(write.wait({ tier: "global" }));
    const read = settled(kept.all(app.tasks));
    fail(failure);

    for (const { error } of [await written, await read]) {
      expect(error).toBeInstanceOf(EncryptionNeedsLocalFirstError);
      expect((error as EncryptionNeedsLocalFirstError).reason).toBe("load-failed");
      expect((error as Error).cause).toBe(failure);
    }
    expect(isThinDb(kept)).toBe(true);
    warned.mockRestore();
  }, 60_000);

  it("ends a stalled load at the deadline", async () => {
    const host = await serve();
    const session = await open(host, undefined, () => new Promise(() => {}));
    const kept = db(session);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });

    const write = kept.insert(app.tasks, { projectId: crypto.randomUUID(), title: "x" } as never);
    const written = settled(write.wait({ tier: "global" }));
    const read = settled(kept.all(app.tasks));
    await vi.advanceTimersByTimeAsync(29_000);
    let ended = false;
    void written.then(() => (ended = true));
    await vi.advanceTimersByTimeAsync(0);
    expect(ended).toBe(false);
    await vi.advanceTimersByTimeAsync(1_000);
    vi.useRealTimers();

    for (const { error } of [await written, await read]) {
      expect(error).toBeInstanceOf(EncryptionNeedsLocalFirstError);
      expect((error as EncryptionNeedsLocalFirstError).reason).toBe("timeout");
    }
  }, 60_000);

  it("ends waiting operations at sign-out", async () => {
    const host = await serve();
    const gate = gatedLoader();
    const session = await open(host, undefined, gate.loader);
    const kept = db(session);

    const write = kept.insert(app.tasks, { projectId: crypto.randomUUID(), title: "x" } as never);
    const written = settled(write.wait({ tier: "global" }));
    const read = settled(kept.all(app.tasks));
    const errors: Error[] = [];
    kept.subscribe(app.tasks, { onUpdate: () => {}, onError: (error) => errors.push(error) });
    await session.logout();

    // The session closes the signed-out Db first: they end with its error.
    for (const { error } of [await written, await read]) {
      expect(error).toBeInstanceOf(DbClosedError);
    }
    expect(errors).toHaveLength(1);
    expect(errors[0]).toBeInstanceOf(DbClosedError);
  }, 60_000);

  it("ends a waiting write the moment the session replaces the Db, before it shuts down", async () => {
    const host = await serve();
    const gate = gatedLoader();
    const session = await open(host, undefined, gate.loader);
    const kept = db(session);
    const write = kept.insert(app.tasks, { projectId: crypto.randomUUID(), title: "x" } as never);
    const written = settled(write.wait({ tier: "global" }));
    await expect.poll(() => gate.calls, { timeout: 10_000 }).toBe(1);

    // The session marks the old Db replaced while its client still drains;
    // the Db is not shut down yet, so only this ends the wait.
    kept.markReplaced("signed-out");

    const { error } = await written;
    expect(error).toBeInstanceOf(DbReplacedError);
    gate.release();
  }, 60_000);

  it("does not run a waiting write after an account switch", async () => {
    const host = await serve();
    const { kept, session, gate, project } = await thinMember(host);
    const write = kept.insert(app.tasks, { projectId: project, title: "never" } as never);
    const written = settled(write.wait({ tier: "global" }));

    await session.logout();
    gate.release();

    expect((await written).error).toBeInstanceOf(DbClosedError);
    const other = await open(host, undefined, loadLocalFirst);
    await expect.poll(() => isThinDb(db(other)), { timeout: 20_000 }).toBe(false);
    // A local-first client reads an encrypted table once it has verified its account.
    await db(other).encryption.ready({ timeoutMs: 20_000 });
    const titles = await db(other).all(tasks.select("projectId").where({ projectId: project }), {
      tier: "remote",
    });
    // Only `kept`, from the earlier visit.
    expect(titles).toHaveLength(1);
  }, 90_000);

  it("reports a waiting write nobody waited for", async () => {
    const host = await serve();
    const warned = vi.spyOn(console, "warn").mockImplementation(() => {});
    let fail!: (error: Error) => void;
    const loading = new Promise<never>((_, reject) => (fail = reject));
    const session = await open(host, undefined, () => loading);
    const kept = db(session);
    const events: MutationErrorEvent[] = [];
    kept.onMutationError((event) => events.push(event));

    kept.insert(app.tasks, { projectId: crypto.randomUUID(), title: "x" } as never);
    fail(new Error("offline"));

    await expect.poll(() => events.length, { timeout: 10_000 }).toBe(1);
    expect(events[0]!.code).toBe("not_sent");
    expect(events[0]!.reason).toContain("Loading the local-first client failed: offline");
    warned.mockRestore();
  }, 60_000);

  it("plain writes behind a failed wait reach the server in order", async () => {
    const host = await serve();
    const warned = vi.spyOn(console, "warn").mockImplementation(() => {});
    let fail!: (error: Error) => void;
    const loading = new Promise<never>((_, reject) => (fail = reject));
    const session = await open(host, undefined, () => loading);
    const kept = db(session);

    const task = kept.insert(app.tasks, { projectId: crypto.randomUUID(), title: "x" } as never);
    const queuedTask = kept.insert(app.tasks, {
      projectId: crypto.randomUUID(),
      title: "y",
    } as never);
    // The reply's insert is allowed only once its note is on the server.
    const note = kept.insert(app.notes, { text: "note" } as never);
    const reply = kept.insert(app.replies, {
      noteId: note.value.id,
      text: "reply",
    } as never);
    const taskWait = settled(task.wait({ tier: "global" }));
    const queuedWait = settled(queuedTask.wait({ tier: "global" }));
    fail(new Error("offline"));

    const failed = (await taskWait).error as EncryptionNeedsLocalFirstError;
    expect(failed).toBeInstanceOf(EncryptionNeedsLocalFirstError);
    expect(failed.reason).toBe("load-failed");
    // Queued behind it: the same error, without a wait of its own.
    expect((await queuedWait).error).toBe(failed);
    await note.wait({ tier: "global" });
    await reply.wait({ tier: "global" });
    expect((await kept.all(app.replies, { tier: "remote" })).map((row) => row.text)).toEqual([
      "reply",
    ]);
    expect(isThinDb(kept)).toBe(true);
    warned.mockRestore();
  }, 60_000);

  it("a live query on an encrypted table emits nothing until local-first, then its rows", async () => {
    const host = await serve();
    const { kept, gate } = await thinMember(host);
    const seen: string[][] = [];
    const errors: Error[] = [];
    const stop = kept.subscribe(app.tasks, {
      onUpdate: (rows) => seen.push(titles(rows)),
      onError: (error) => errors.push(error),
    });
    // The thin client answers a plain read meanwhile, so the query had its chance.
    await kept.all(app.notes, { tier: "remote" });
    expect(seen).toEqual([]);

    gate.release();
    await expect.poll(() => isThinDb(kept), { timeout: 20_000 }).toBe(false);
    await expect.poll(() => seen.at(-1), { timeout: 20_000 }).toEqual(["kept"]);
    expect(seen.every((rows) => rows.length > 0)).toBe(true);
    expect(errors).toEqual([]);
    stop();
  }, 90_000);

  it("a parked live query warns at the deadline and opens when the upgrade lands", async () => {
    const host = await serve();
    const gate = gatedLoader();
    const session = await open(host, undefined, gate.loader);
    const kept = db(session);
    const warned = vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const seen: unknown[][] = [];
    const errors: Error[] = [];
    kept.subscribe(app.tasks, {
      onUpdate: (rows) => seen.push(rows),
      onError: (error) => errors.push(error),
    });

    await vi.advanceTimersByTimeAsync(30_000);
    await vi.advanceTimersByTimeAsync(60_000);
    vi.useRealTimers();
    const warnings = warned.mock.calls.filter(([message]) =>
      String(message).includes("A live query on `tasks`"),
    );
    expect(warnings).toHaveLength(1);
    expect(seen).toEqual([]);
    expect(errors).toEqual([]);

    gate.release();
    await expect.poll(() => seen.length, { timeout: 20_000 }).toBeGreaterThan(0);
    expect(seen.at(-1)).toEqual([]);
    expect(errors).toEqual([]);
  }, 90_000);

  it("reruns a refused callback transaction once on the local-first client", async () => {
    const host = await serve();
    const { kept, gate, project } = await thinMember(host);
    let runs = 0;
    const result = kept.transaction((tx) => {
      runs++;
      tx.insert(app.notes, { text: "in the transaction" } as never);
      tx.insert(app.tasks, { projectId: project, title: "transacted" } as never);
      return runs;
    });
    // The load started with the refusal; the rerun waits for it.
    gate.release();

    const written = await result;
    expect(written.value).toBe(2);
    await written.wait({ tier: "global" });
    expect(runs).toBe(2);
    expect((await kept.all(app.notes, { tier: "remote" })).map((row) => row.text)).toEqual([
      "in the transaction",
    ]);
    const rows = await kept.all(tasks.where({ projectId: project }));
    expect(titles(rows).sort()).toEqual(["kept", "transacted"]);
  }, 90_000);

  it("does not rerun a callback transaction twice", async () => {
    const host = await serve();
    const gate = gatedLoader();
    const session = await open(host, undefined, gate.loader);
    const kept = db(session);
    const again = new EncryptionNeedsLocalFirstError({
      reason: "load-failed",
      operation: "insert",
      table: "tasks",
    });
    let runs = 0;
    const result = settled(
      kept.transaction((tx) => {
        runs++;
        if (runs > 1) throw again;
        tx.insert(app.tasks, { projectId: crypto.randomUUID(), title: "x" } as never);
      }),
    );
    gate.release();

    expect((await result).error).toBe(again);
    expect(runs).toBe(2);
  }, 60_000);

  it("an explicit transaction says to begin again, and starts the load", async () => {
    const host = await serve();
    pageStillLoading();
    const gate = gatedLoader();
    const session = await open(host, undefined, gate.loader);
    const kept = db(session);

    const tx = kept.beginTransaction();
    tx.insert(app.notes, { text: "staged" } as never);
    let refused: unknown;
    try {
      tx.insert(app.tasks, { projectId: crypto.randomUUID(), title: "x" } as never);
    } catch (error) {
      refused = error;
    }
    expect(refused).toBeInstanceOf(EncryptionNeedsLocalFirstError);
    expect((refused as EncryptionNeedsLocalFirstError).reason).toBe("transaction");
    expect((refused as Error).message).toContain(
      "begin it again after `await db.upgradeToLocalFirst(loader)`",
    );
    const committed = await settled(
      Promise.resolve().then(() => tx.commit().wait({ tier: "global" })),
    );
    expect(committed.error).toBe(refused);
    await expect.poll(() => gate.calls, { timeout: 10_000 }).toBe(1);
  }, 60_000);

  it("a device awaiting approval runs a waited write at once, with the local-first client's answer", async () => {
    const host = await serve();
    const first = await userDevice(host, "alice", loadLocalFirst);
    await expect.poll(() => isThinDb(db(first)), { timeout: 20_000 }).toBe(false);
    expect(await db(first).encryption.ready({ timeoutMs: 20_000 })).toBe("active");
    const gate = gatedLoader();
    const second = await userDevice(host, "alice", gate.loader);
    const kept = db(second);

    const write = kept.insert(app.tasks, {
      projectId: crypto.randomUUID(),
      title: "unapproved",
    } as never);
    const started = Date.now();
    const written = settled(write.wait({ tier: "global" }));
    gate.release();
    await expect.poll(() => isThinDb(kept), { timeout: 20_000 }).toBe(false);
    expect(await kept.encryption.ready({ timeoutMs: 20_000 })).toBe("awaiting-approval");

    const { error } = await written;
    // Not held to the deadline: it ran on the local-first client, which answered.
    expect(Date.now() - started).toBeLessThan(25_000);
    expect(error).toBeDefined();
    expect(error).not.toBeInstanceOf(EncryptionNeedsLocalFirstError);
  }, 90_000);

  it("a plain-only select keeps a row whose cell is key-unavailable across the upgrade", async () => {
    const host = await serve();
    // Alice's task, in a space Bob holds no key for.
    await member(host);
    const gate = gatedLoader();
    const bob = await open(host, undefined, gate.loader);
    const kept = db(bob);
    const plain = tasks.select("projectId", "note").where({});

    const thin = await kept.all(plain, { tier: "remote" });
    expect(thin).toHaveLength(1);
    gate.release();
    await expect.poll(() => isThinDb(kept), { timeout: 20_000 }).toBe(false);
    // A local-first client reads an encrypted table once it has verified its account.
    expect(await kept.encryption.ready({ timeoutMs: 20_000 })).toBe("active");

    const local = await kept.all(plain, { tier: "remote" });
    expect(local).toEqual(thin);
  }, 90_000);

  it("a callback transaction begun while a write waits runs once more on the local-first client", async () => {
    const host = await serve();
    const { kept, gate, project } = await thinMember(host);
    const task = kept.insert(app.tasks, { projectId: project, title: "first" } as never);
    const taskId = idOf(task.value);
    // Admitted behind the waiting write, it begins only after the hand-over,
    // when the thin runtime has closed.
    let runs = 0;
    const result = kept.transaction((tx) => {
      runs++;
      tx.insert(app.comments, { taskId, text: "in a transaction" } as never);
    });
    gate.release();

    await task.wait({ tier: "global" });
    await (await result).wait({ tier: "global" });
    expect(runs).toBe(2);
    expect(texts(await kept.all(app.comments, { tier: "remote" }))).toEqual(["in a transaction"]);
  }, 90_000);

  it("reruns a callback transaction at most once, whichever ends each run", async () => {
    const host = await serve();
    const gate = gatedLoader();
    const session = await open(host, undefined, gate.loader);
    const kept = db(session);
    const closed = new Error("Begin transaction failed", {
      cause: new RuntimeClosedError("the thin runtime is closed"),
    });
    // Refused on the thin client, then ended as if by a closed runtime.
    let runs = 0;
    const refusedThenClosed = settled(
      kept.transaction((tx) => {
        runs++;
        if (runs > 1) throw closed;
        tx.insert(app.tasks, { projectId: crypto.randomUUID(), title: "x" } as never);
      }),
    );
    gate.release();
    expect((await refusedThenClosed).error).toBe(closed);
    expect(runs).toBe(2);
    expect(isThinDb(kept)).toBe(false);
  }, 60_000);

  it("reruns a callback transaction ended by the hand-over at most once", async () => {
    const host = await serve();
    const gate = gatedLoader();
    const session = await open(host, undefined, gate.loader);
    const kept = db(session);
    const refused = new EncryptionNeedsLocalFirstError({
      reason: "transaction",
      operation: "insert",
      table: "tasks",
    });
    // A write waits; the transaction is admitted behind it, and ended by the
    // hand-over. Its rerun is refused as if it were on a thin client.
    kept.insert(app.tasks, { projectId: crypto.randomUUID(), title: "x" } as never);
    let runs = 0;
    const result = settled(
      kept.transaction((tx) => {
        runs++;
        if (runs > 1) throw refused;
        tx.insert(app.notes, { text: "plain" } as never);
      }),
    );
    gate.release();
    expect((await result).error).toBe(refused);
    expect(runs).toBe(2);
  }, 60_000);

  describe("db.encryption while thin", () => {
    it("a handle taken while thin answers after the upgrade", async () => {
      const host = await serve();
      const { session, kept, gate } = await thinMember(host);
      const handle = kept.encryption;
      const stored = handle.deviceId();
      const listed = handle.devices.list();
      await expect.poll(() => gate.calls, { timeout: 10_000 }).toBe(1);
      gate.release();

      const id = await stored;
      expect(id).toEqual(expect.any(String));
      expect((await listed).some((device) => device.id === id && device.isThisDevice)).toBe(true);
      expect(isThinDb(kept)).toBe(false);
      // The handle taken while thin is still good, and so is the one now.
      await expect(handle.deviceId()).resolves.toBe(id);
      await expect(db(session).encryption.deviceId()).resolves.toBe(id);
    }, 90_000);

    it("ready() starts the upgrade", async () => {
      const host = await serve();
      const { device } = await member(host);
      pageStillLoading();
      const gate = gatedLoader();
      const session = await open(host, device, gate.loader);
      const kept = db(session);
      expect(gate.calls).toBe(0);

      const ready = kept.encryption.ready({ timeoutMs: 20_000 });
      await expect.poll(() => gate.calls, { timeout: 10_000 }).toBe(1);
      gate.release();
      await expect(ready).resolves.toBe("active");
      expect(isThinDb(kept)).toBe(false);
    }, 90_000);

    it("attaches listeners without starting the load, and they fire after a later upgrade", async () => {
      const host = await serve();
      const { device } = await member(host);
      // The page is still loading, so no background upgrade starts either.
      pageStillLoading();
      const gate = gatedLoader();
      const kept = db(await open(host, device, gate.loader));
      const unreadable: unknown[] = [];
      const stop = kept.encryption.onUnreadable((report) => unreadable.push(report));
      const stopped = kept.encryption.onChallenge(() => {});
      kept.encryption.onReport(() => {});
      stopped();
      // Attached at start-up, they must not pull in the local-first client.
      await new Promise((resolve) => setTimeout(resolve, 300));
      expect(gate.calls).toBe(0);
      expect(isThinDb(kept)).toBe(true);

      // Some other cause starts the upgrade; the listeners attach with it.
      const upgrading = kept.upgradeToLocalFirst(gate.loader);
      gate.release();
      await upgrading;
      await expect.poll(() => isThinDb(kept), { timeout: 20_000 }).toBe(false);
      const real = kept.encryption as unknown as { listeners: Set<unknown> };
      await expect.poll(() => real.listeners.size, { timeout: 10_000 }).toBe(2);
      stop();
      expect(real.listeners.size).toBe(1);
    }, 90_000);

    it("listeners of a Db that closes without upgrading are dropped, and never fire", async () => {
      const host = await serve();
      const { device } = await member(host);
      pageStillLoading();
      const gate = gatedLoader();
      const session = await open(host, device, gate.loader);
      const kept = db(session);
      let fired = 0;
      kept.encryption.onReport(() => fired++);
      const waiters = () =>
        (kept as unknown as { successorWaiters: Set<unknown> }).successorWaiters;
      expect(waiters().size).toBe(1);

      await session.close();
      expect(waiters().size).toBe(0);
      expect(fired).toBe(0);
      expect(gate.calls).toBe(0);
    }, 90_000);

    it("refuses the synchronous recovery.create() and starts the load", async () => {
      const host = await serve();
      const gate = gatedLoader();
      const session = await open(host, undefined, gate.loader);
      const kept = db(session);

      let thrown: unknown;
      try {
        kept.encryption.recovery.create();
      } catch (error) {
        thrown = error;
      }
      expect(thrown).toBeInstanceOf(EncryptionNeedsLocalFirstError);
      expect((thrown as EncryptionNeedsLocalFirstError).reason).toBe("not-loaded");
      expect((thrown as Error).message).toContain(
        "call it again after `await db.encryption.ready()`",
      );
      await expect.poll(() => gate.calls, { timeout: 10_000 }).toBe(1);
    }, 60_000);

    it("refuses without a loader, like any operation", async () => {
      const host = await serve();
      vi.spyOn(console, "error").mockImplementation(() => {});
      const session = await open(host);
      const kept = db(session);

      const { error } = await settled(kept.encryption.deviceId());
      expect(error).toBeInstanceOf(EncryptionNeedsLocalFirstError);
      expect((error as EncryptionNeedsLocalFirstError).reason).toBe("no-loader");
      expect(() => kept.encryption.recovery.create()).toThrow(EncryptionNeedsLocalFirstError);
    }, 60_000);

    it("ends a waiting call at sign-out", async () => {
      const host = await serve();
      const gate = gatedLoader();
      const session = await open(host, undefined, gate.loader);
      const waiting = settled(db(session).encryption.deviceId());
      await expect.poll(() => gate.calls, { timeout: 10_000 }).toBe(1);
      await session.logout();
      expect((await waiting).error).toBeInstanceOf(DbClosedError);
    }, 60_000);

    it("an app that declares no encryption still gets EncryptionNotDeclaredError", async () => {
      const host = await serve();
      const session = await openBrowserSessionCore({
        scope: host.jazz.appId,
        serverUrl: host.serverUrl,
        store: memoryStore(),
        initial: "device",
        driver: { type: "memory" },
      });
      cleanups.push(() => session.close());
      const kept = db(session);
      expect(isThinDb(kept)).toBe(true);
      expect(() => kept.encryption.devices).toThrow(EncryptionNotDeclaredError);
    }, 60_000);
  });
});
