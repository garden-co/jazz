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
import { DbReplacedError } from "../runtime/db-replaced-error.js";
import { EncryptionNeedsLocalFirstError } from "../runtime/encryption-needs-local-first.js";
import { isThinDb, type LocalFirstLoader } from "../runtime/local-first-upgrade.js";
import { testApp } from "../testing/index.js";
import { openBrowserSessionCore, type BrowserJazzSession } from "./create-jazz-session.js";

// A handle taken from `db.encryption` while the Db is thin keeps working after
// the local-first upgrade, and reports the Db's own error after sign-out.

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

/** A loader that loads only once `release()` is called. */
function gatedLoader() {
  let release!: () => void;
  const released = new Promise<void>((resolve) => (release = resolve));
  return {
    release: () => release(),
    loader: (async () => {
      await released;
      return { createClient: createLocalFirstDb };
    }) as LocalFirstLoader,
  };
}

/** A thin session of an account the app signs in itself. */
async function thinSession(host: Host, client: LocalFirstLoader): Promise<BrowserJazzSession> {
  const session = await openBrowserSessionCore({
    scope: host.jazz.appId,
    serverUrl: host.serverUrl,
    store: memoryStore(),
    driver: { type: "memory" },
    bearer: () => "token-alice",
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

describe("a db.encryption handle taken while thin", () => {
  it("keeps working after the upgrade: recovery.create() runs and stop() detaches", async () => {
    const host = await serve();
    const gate = gatedLoader();
    const session = await thinSession(host, gate.loader);
    const kept = db(session);
    expect(isThinDb(kept)).toBe(true);
    const handle = kept.encryption;
    const refused = sync(() => handle.recovery.create());
    expect(refused).toBeInstanceOf(EncryptionNeedsLocalFirstError);
    expect((refused as EncryptionNeedsLocalFirstError).reason).toBe("not-loaded");
    handle.onReport(() => {});

    gate.release();
    await handle.ready({ timeoutMs: 20_000 });
    expect(isThinDb(kept)).toBe(false);

    const real = kept.encryption as unknown as { listeners: Set<unknown> };
    expect(real.listeners.size).toBe(1);
    expect(handle.recovery.create().split(" ")).toHaveLength(24);
    handle.stop();
    expect(real.listeners.size).toBe(0);
  }, 90_000);

  it("reports the Db's replaced error after sign-out, not not-loaded", async () => {
    const host = await serve();
    const gate = gatedLoader();
    const session = await thinSession(host, gate.loader);
    const handle = db(session).encryption;
    await session.logout();

    expect(sync(() => handle.recovery.create())).toBeInstanceOf(DbReplacedError);
    await expect(handle.deviceId()).rejects.toBeInstanceOf(DbReplacedError);
  }, 60_000);
});
