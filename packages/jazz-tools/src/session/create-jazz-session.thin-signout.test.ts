import { mkdtemp } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { afterEach, expect, it } from "vitest";
import { acceptDeviceAccounts, createJazz, sqlite } from "../backend/index.js";
import type { AccountStore } from "../accounts/persistence.js";
import { memoryPrivateStore } from "../encryption/index.js";
import { createDb as createLocalFirstDb } from "../runtime/default-create-db.js";
import type { Db } from "../runtime/db.js";
import { DbReplacedError } from "../runtime/db-replaced-error.js";
import { DbClosedError } from "../runtime/graceful-shutdown-error.js";
import type { LocalFirstLoader } from "../runtime/local-first-upgrade.js";
import { testApp } from "../testing/index.js";
import { openBrowserSessionCore, type BrowserJazzSession } from "./create-jazz-session.js";

// What a write waiting for the local-first client gets when the session signs
// out, through the real sign-out: the client shuts down first, and only then
// is its Db marked replaced.

const app = testApp({
  format: 1,
  tables: {
    projects: { columns: { name: "string", ownerId: "uuid" } },
    tasks: {
      columns: { projectId: "uuid", title: "bytes", __e2ee_eq_title: "bytes" },
      relations: { project: { rel: "projects", column: "projectId" } },
      encryption: {
        space: "projectId",
        scope: "projects",
        columns: { title: "string" },
        equality: ["title"],
      },
    },
  },
});
const permissions = {
  $schema: "https://jazz.tools/schemas/jazz.permissions.v1.json",
  tables: {
    projects: {
      read: "exists __e2ee_members(scopeId = table_id('projects'), identifier = this.id, accountId = session.user)",
      insert: "$createdBy.account = session.user",
    },
    tasks: { read: "true", insert: "true" },
  },
} as never;

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
});

async function serve() {
  const dir = await mkdtemp(`${tmpdir()}/thin-signout-`);
  const jazz = await createJazz({
    app: app as never,
    permissions,
    store: sqlite({ path: `${dir}/core.sqlite` }),
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
  };
}

function memoryStore(): AccountStore {
  let stored: string | null = null;
  return {
    read: async () => stored,
    update: async (transform) => void (stored = transform(stored)),
  };
}

it("ends a write waiting for the local-first client with DbClosedError at the real sign-out, and later calls get DbReplacedError", async () => {
  const host = await serve();
  let calls = 0;
  let release!: () => void;
  const released = new Promise<void>((resolve) => (release = resolve));
  const loader = (async () => {
    calls++;
    await released;
    return { createClient: createLocalFirstDb };
  }) as LocalFirstLoader;
  const session: BrowserJazzSession = await openBrowserSessionCore({
    scope: host.jazz.appId,
    serverUrl: host.serverUrl,
    store: memoryStore(),
    driver: { type: "memory" },
    bearer: () => "token-alice",
    encryption: { app, store: memoryPrivateStore() },
    client: loader,
  } as never);
  cleanups.push(() => session.close());
  const kept: Db = session.getSnapshot().client!.db;

  const write = kept.insert(app.tasks, { projectId: crypto.randomUUID(), title: "x" } as never);
  const written = write.wait({ tier: "global" }).then(
    () => undefined,
    (error: unknown) => error,
  );
  await expect.poll(() => calls, { timeout: 10_000 }).toBe(1);

  await session.logout();

  // The client shuts down before the session marks its Db replaced, so the wait ends with the shutdown's error.
  const error = await written;
  expect(error).toBeInstanceOf(DbClosedError);
  expect(error).not.toBeInstanceOf(DbReplacedError);
  // Once the sign-out is done, the Db reports that it was replaced.
  await expect(kept.encryption.deviceId()).rejects.toBeInstanceOf(DbReplacedError);
  release();
}, 60_000);
