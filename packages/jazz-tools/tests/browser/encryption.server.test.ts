import { afterEach, expect, it } from "vitest";
import { createJazzClient } from "../../src/session/create-jazz-session.js";
import { createDb as createLocalFirstDb } from "../../src/runtime/default-create-db.js";
import { isThinDb } from "../../src/runtime/local-first-upgrade.js";
import { uniqueDbName, waitForCondition } from "./support.js";
import {
  deploy,
  getJazzServerInfo,
  jazzAppFetch,
  jazzAppVisitor,
  startJazzApp,
} from "./testing-server.js";

// End-to-end encryption in a real browser: a device account whose keys live in
// the browser's own private store (IndexedDB, a non-extractable WebCrypto key),
// the encrypted rows kept by the shared worker as sealed bytes, a real server
// relaying them. The account's store is localStorage; reloading is closing the
// session and opening another.

import { app } from "./apps/encrypted-tab/jazz.generated.js";
import permissions from "./apps/encrypted-tab/jazz.permissions.json";

const membersTable = (app as never as Record<string, never>)["__e2ee_members"];

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup().catch(() => {});
  localStorage.clear();
});

async function serverAt(name: string) {
  const { appId, serverUrl: coreUrl, adminSecret } = await getJazzServerInfo(uniqueDbName(name));
  await deploy({ appId, serverUrl: coreUrl, adminSecret, app, permissions });
  const backend = await startJazzApp(
    { appId, serverUrl: coreUrl, adminSecret },
    { schema: app, permissions },
  );
  const url = await jazzAppVisitor(backend);
  const pageFetch = globalThis.fetch;
  globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    const target = input instanceof Request ? input.url : String(input);
    return target.startsWith(new URL(url).origin)
      ? jazzAppFetch(input, init)
      : pageFetch(input, init);
  }) as typeof fetch;
  cleanups.push(async () => {
    globalThis.fetch = pageFetch;
  });
  return { url, appId };
}

/** A page load: a session with a device account and the app's encryption. */
async function load(server: { url: string; appId: string }, dbName: string) {
  const session = await createJazzClient({
    scope: server.appId,
    serverUrl: server.url,

    driver: { type: "persistent", dbName },
    fetch: jazzAppFetch,
    initial: "device",
    client: async () => ({ createClient: createLocalFirstDb }),
    encryption: { app },
  } as never);
  cleanups.push(() => session.close());
  await waitForCondition(
    async () => {
      const client = session.getSnapshot().client;
      return !!client && !isThinDb(client.db);
    },
    30_000,
    "The session should upgrade to local-first",
  );
  return session;
}

/** Names the step that does not finish, instead of a bare test timeout. */
function step<T>(name: string, work: Promise<T>, ms = 15_000): Promise<T> {
  return Promise.race([
    work,
    new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error(`step "${name}" did not finish in ${ms} ms`)), ms),
    ),
  ]);
}

it("encrypts in a real browser: a device's keys in the browser's own store, a tab behind the shared worker, rows sealed on the server, readable by the device, the same device after a reload", async () => {
  const server = await serverAt("encryption-browser");
  const dbName = uniqueDbName("encryption-browser-db");
  const first = await step("load", load(server, dbName), 40_000);
  const db = first.getSnapshot().client!.db;
  const ready = await step("ready", db.encryption.ready({ timeoutMs: 40_000 }), 45_000);
  expect(ready).toBe("active");
  const device = await step("deviceId", db.encryption.deviceId());
  const account = first.getSnapshot().user!.accountId;

  const id = crypto.randomUUID();
  const self = await step(
    "member row",
    db.encryption.members.row({ table: "projects", id }, account),
  );
  const written = await db.transaction((tx) => {
    tx.insert(app.projects, { name: "plans", ownerId: account }, { id });
    tx.insert(membersTable, self as never);
  });
  await step("project global", written.wait({ tier: "global" }));
  await step(
    "task global",
    db.insert(app.tasks, { projectId: id, title: "a secret title" }).wait({ tier: "global" }),
  );
  const read = async (client: typeof db) =>
    await client.all(app.tasks.where({ projectId: id }), { tier: "local-first" });
  expect((await step("read", read(db))).map((row) => row.title)).toEqual(["a secret title"]);

  // The device's keys are in the browser's store, protected by a key that
  // cannot be exported.
  const names = await indexedDB.databases();
  expect(names.map((entry) => entry.name)).toContain("jazz-private-store");

  await first.close();
  const reloaded = await step("reload", load(server, dbName), 40_000);
  const again = reloaded.getSnapshot().client!.db;
  expect(await step("ready again", again.encryption.ready({ timeoutMs: 150_000 }), 160_000)).toBe(
    "active",
  );
  expect(await again.encryption.deviceId()).toBe(device);
  await waitForCondition(
    async () => (await read(again)).some((row) => row.title === "a secret title"),
    30_000,
    "The reloaded device should read its encrypted row",
  );
}, 240_000);
