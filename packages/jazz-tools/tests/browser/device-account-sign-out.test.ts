import { afterEach, expect, it } from "vitest";
import { deploy } from "./deploy.js";
import { createJazzClient } from "../../src/session/create-jazz-session.js";
import { createDb as createLocalFirstDb } from "../../src/runtime/default-create-db.js";
import { isThinDb } from "../../src/runtime/local-first-upgrade.js";
import { NoKeptDeviceAccountError } from "../../src/session/no-kept-device-account-error.js";
import { uniqueDbName, waitForCondition } from "./support.js";
import {
  sessionTabApp as app,
  sessionTabPermissions as permissions,
} from "./session-tab-schema.js";
import { getJazzServerInfo, jazzAppFetch, jazzAppVisitor, startJazzApp } from "./testing-server.js";

// E7: signing out of a device account keeps its key (except with
// clearOnSignOut), a reload stays signed out, and `reopenDeviceAccount()`
// resumes it with its id and its data. In a real browser, with the account
// store in localStorage and the data in IndexedDB.

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup().catch(() => {});
  localStorage.clear();
});

async function visitorAt(name: string) {
  const { appId, serverUrl: coreUrl, adminSecret } = await getJazzServerInfo(uniqueDbName(name));
  await deploy({ appId, serverUrl: coreUrl, adminSecret, schema: app.wasmSchema, permissions });
  const backend = await startJazzApp(
    { appId, serverUrl: coreUrl, adminSecret },
    { schema: app.wasmSchema, permissions },
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
  return url;
}

/** A page load: a session with `initial: "device"`. */
async function load(url: string, clearOnSignOut = false) {
  const session = await createJazzClient({
    serverUrl: url,
    fetch: jazzAppFetch,
    initial: "device",
    clearOnSignOut,
    client: async () => ({ createClient: createLocalFirstDb }),
  });
  cleanups.push(() => session.close());
  return session;
}

async function localFirst(session: Awaited<ReturnType<typeof load>>) {
  await waitForCondition(
    async () => {
      const client = session.getSnapshot().client;
      return !!client && !isThinDb(client.db);
    },
    30_000,
    "The session should upgrade to local-first",
  );
}

it("keeps a signed-out device account to reopen, with its id and rows, across a reload (E7)", async () => {
  const url = await visitorAt("device-account-sign-out-reopen");
  const first = await load(url);
  await localFirst(first);
  const account = first.getSnapshot().user!.accountId;
  await first.db.insert(app.tasks, { title: "before sign-out" }).wait({ tier: "local" });
  const key = first.exportDeviceKey();
  await first.signOut();
  expect(first.getSnapshot().status).toBe("signed-out");
  // Signed out, the session still exports the kept key.
  expect(first.exportDeviceKey()).toBe(key);
  await first.close();

  // The reload stays signed out: no new device account over the sign-out.
  const reloaded = await load(url);
  expect(reloaded.getSnapshot().status).toBe("signed-out");
  expect(reloaded.exportDeviceKey()).toBe(key);
  await reloaded.reopenDeviceAccount();
  expect(reloaded.getSnapshot()).toMatchObject({
    status: "ready",
    user: { accountId: account, kind: "device" },
  });
  await localFirst(reloaded);
  await waitForCondition(
    async () => (await reloaded.db.all(app.tasks)).some((task) => task.title === "before sign-out"),
    15_000,
    "The reopened device account should read its row",
  );

  // createDeviceAccount() still starts a fresh one on purpose.
  await reloaded.createDeviceAccount();
  expect(reloaded.getSnapshot().user!.accountId).not.toBe(account);
}, 120_000);

it("keeps no key after a clearOnSignOut sign-out: nothing to reopen or export (E7)", async () => {
  const url = await visitorAt("device-account-sign-out-clear");
  const first = await load(url, true);
  await localFirst(first);
  await first.signOut();
  expect(first.getSnapshot().status).toBe("signed-out");
  expect(() => first.exportDeviceKey()).toThrow(NoKeptDeviceAccountError);
  await expect(first.reopenDeviceAccount()).rejects.toThrow(NoKeptDeviceAccountError);
  await first.close();

  const reloaded = await load(url, true);
  expect(reloaded.getSnapshot().status).toBe("signed-out");
  expect(() => reloaded.exportDeviceKey()).toThrow(/keeps no device account/);
}, 120_000);
