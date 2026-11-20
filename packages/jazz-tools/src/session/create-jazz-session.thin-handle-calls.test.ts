import { mkdtemp } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { afterEach, describe, expect, it, vi } from "vitest";
import { acceptDeviceAccounts, createJazz, sqlite } from "../backend/index.js";
import type { AccountStore } from "../accounts/persistence.js";
import { memoryPrivateStore } from "../encryption/index.js";
import { createDb as createLocalFirstDb } from "../runtime/default-create-db.js";
import type { Db } from "../runtime/db.js";
import { EncryptionNeedsLocalFirstError } from "../runtime/encryption-needs-local-first.js";
import { isThinDb, type LocalFirstLoader } from "../runtime/local-first-upgrade.js";
import { testApp } from "../testing/index.js";
import { openBrowserSessionCore, type BrowserJazzSession } from "./create-jazz-session.js";

// Calls on a db.encryption handle taken while thin: what starts the load, what a listener receives, and how a call ends at a failed or a stalled load.

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

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
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

/** A thin session of an account the app signs in itself. */
async function thinSession(host: Host, client: LocalFirstLoader): Promise<BrowserJazzSession> {
  const session = await openBrowserSessionCore({
    scope: host.jazz.appId,
    serverUrl: host.serverUrl,
    store: memoryStore(),
    driver: { type: "memory" },
    initial: "device",
    encryption: { app, store: memoryPrivateStore() },
    client,
  } as never);
  cleanups.push(() => session.close());
  return session;
}

const db = (session: BrowserJazzSession): Db => session.getSnapshot().client!.db;

function sync(run: () => unknown): unknown {
  try {
    run();
  } catch (error) {
    return error;
  }
  return undefined;
}

describe("calls on a db.encryption handle taken while thin", () => {
  it("recovery.create() refuses and itself starts the load", async () => {
    const host = await serve();
    // No background upgrade: only the call can start the load.
    pageStillLoading();
    const gate = gatedLoader();
    const kept = db(await thinSession(host, gate.loader));
    expect(isThinDb(kept)).toBe(true);
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(gate.calls).toBe(0);

    const refused = sync(() => kept.encryption.recovery.create());
    expect(refused).toBeInstanceOf(EncryptionNeedsLocalFirstError);
    expect((refused as EncryptionNeedsLocalFirstError).reason).toBe("not-loaded");
    await expect.poll(() => gate.calls, { timeout: 10_000 }).toBe(1);
  }, 60_000);

  it("delivers reports to listeners attached while thin, and not to a stopped one", async () => {
    const host = await serve();
    const gate = gatedLoader();
    const kept = db(await thinSession(host, gate.loader));
    const handle = kept.encryption;
    const reports: unknown[] = [];
    const unreadable: unknown[] = [];
    let challenged = 0;
    let stoppedHeard = 0;
    handle.onReport((report) => reports.push(report));
    handle.onUnreadable((report) => unreadable.push(report));
    handle.onChallenge(() => challenged++);
    handle.onReport(() => stoppedHeard++)();

    const upgrading = kept.upgradeToLocalFirst(gate.loader);
    gate.release();
    await upgrading;
    await expect.poll(() => isThinDb(kept), { timeout: 20_000 }).toBe(false);
    // The runtime reports once each; the listeners attached while thin hear them.
    const sent = [
      {
        type: "unreadable",
        table: "tasks",
        id: null,
        column: "title",
        code: "no-key",
        space: null,
      },
      { type: "challenged" },
    ];
    const native = (kept.encryption as unknown as { native: { takeEncryptionReports(): string[] } })
      .native;
    let pending = sent.map((report) => JSON.stringify(report));
    native.takeEncryptionReports = () => pending.splice(0);
    await expect.poll(() => challenged, { timeout: 10_000 }).toBe(1);
    expect(reports).toEqual(sent);
    expect(unreadable).toEqual([sent[0]]);
    expect(stoppedHeard).toBe(0);
  }, 90_000);

  it("ends a call at a failed load with the load's failure", async () => {
    const host = await serve();
    pageStillLoading();
    const cause = new Error("chunk failed");
    const kept = db(await thinSession(host, (async () => Promise.reject(cause)) as never));
    const error = await kept.encryption.deviceId().then(
      () => undefined,
      (e: unknown) => e,
    );
    expect(error).toBeInstanceOf(EncryptionNeedsLocalFirstError);
    expect((error as EncryptionNeedsLocalFirstError).reason).toBe("load-failed");
    expect((error as Error).cause).toBe(cause);
    expect(isThinDb(kept)).toBe(true);
  }, 60_000);

  it("applies the caller's shorter ready({ timeoutMs }) while thin", async () => {
    const host = await serve();
    pageStillLoading();
    const gate = gatedLoader();
    const kept = db(await thinSession(host, gate.loader));
    const started = Date.now();
    const error = await kept.encryption.ready({ timeoutMs: 300 }).then(
      () => undefined,
      (e: unknown) => e,
    );
    expect(error).toBeInstanceOf(EncryptionNeedsLocalFirstError);
    expect((error as EncryptionNeedsLocalFirstError).reason).toBe("timeout");
    expect(Date.now() - started).toBeLessThan(10_000);
    expect(isThinDb(kept)).toBe(true);
  }, 30_000);

  it("ends a call at a stalled load at the deadline, and at sign-out", async () => {
    const host = await serve();
    pageStillLoading();
    const gate = gatedLoader();
    const session = await thinSession(host, gate.loader);
    const kept = db(session);
    const settle = (promise: Promise<unknown>) =>
      promise.then(
        () => undefined,
        (e: unknown) => e,
      );

    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    let done = false;
    const timedOut = settle(kept.encryption.deviceId()).then((e) => ((done = true), e));
    await vi.waitFor(() => expect(gate.calls).toBe(1));
    await vi.advanceTimersByTimeAsync(29_000);
    expect(done).toBe(false);
    await vi.advanceTimersByTimeAsync(1_500);
    const error = await timedOut;
    expect(error).toBeInstanceOf(EncryptionNeedsLocalFirstError);
    expect((error as EncryptionNeedsLocalFirstError).reason).toBe("timeout");
    vi.useRealTimers();

    // A call still waiting when the session signs out ends there too.
    const waiting = settle(kept.encryption.devices.list());
    await session.logout();
    expect(await waiting).toBeInstanceOf(Error);
  }, 90_000);
});
