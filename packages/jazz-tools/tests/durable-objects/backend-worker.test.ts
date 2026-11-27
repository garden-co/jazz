// The all-Cloudflare deployment: an app's createJazz backend in a Durable
// Object of the same Worker as its core, on jazz-wasm (no jazz-napi), in
// local workerd. Writes and reads through the app's own routes, and its
// data across a restart.
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import { createJazzClient } from "../../src/backend/index.js";
import { createTestAccountManager } from "../../src/runtime/testing/account-fixtures.js";
import { createClient as createThinClient } from "../../src/thin/index.js";
import { testPermissions } from "../../src/testing/index.js";
import { app } from "./backend-app.generated.js";
import { bundleWorker } from "./bundle.js";
import { hostBuilt } from "./host.js";
import { startDurableObjectCore, type DurableObjectCore } from "./harness.js";

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
});

describe.skipIf(!hostBuilt && !process.env.CI)("createJazz in a Cloudflare Worker", () => {
  let script: string;
  beforeAll(async () => {
    script = await bundleWorker(join(__dirname, "backend-worker.ts"));
  });

  async function worker(
    persist?: string,
    bindings?: Record<string, string>,
  ): Promise<DurableObjectCore> {
    const started = await startDurableObjectCore({ script, backend: true, persist, bindings });
    cleanups.push(() => started.dispose());
    return started;
  }

  it("writes and reads through the app's routes on jazz.db", async () => {
    const { url } = await worker();
    const posted = await fetch(`${url}/todos`, { method: "POST", body: "first" });
    expect(posted.status, await posted.clone().text()).toBe(200);
    expect(await (await fetch(`${url}/todos`)).json()).toEqual(["first"]);
  });

  it.each(["remote", "durableObjects"])("uses %s to reach a core by URL", async (provider) => {
    const core = await startDurableObjectCore({
      script: await bundleWorker(join(__dirname, "real-core-worker.ts")),
    });
    cleanups.push(() => core.dispose());
    const { url } = await worker(undefined, {
      TEST_CORE_URL: core.url,
      ...(provider === "remote" && { TEST_REMOTE_APP_ID: await core.appId() }),
    });
    const write = await fetch(`${url}/todos`, { method: "POST", body: "across workers" });
    expect(write.status, await write.clone().text()).toBe(200);
    expect(await (await fetch(`${url}/todos`)).json()).toEqual(["across workers"]);
  });

  it.each(["..", "a/b", "a\nb", ""])("refuses an invalid discovered app id %j", async (appId) => {
    const core = await discoveryServer(() => Response.json({ appId }));
    const { url } = await worker(undefined, { TEST_CORE_URL: core.url });
    const response = await fetch(`${url}/app-id`);
    expect(response.status).toBe(500);
    expect(await response.text()).toContain(
      "did not answer as a Jazz Durable Object core (GET /app: 200)",
    );
    expect(core.requests).toEqual(["/app"]);
  });

  it.each([301, 302, 303, 307, 308])(
    "refuses a %i redirect during app discovery",
    async (status) => {
      const target = await discoveryServer(() => Response.json({}));
      const core = await discoveryServer(() => Response.redirect(`${target.url}/app`, status));
      const { url } = await worker(undefined, { TEST_CORE_URL: core.url });
      const response = await fetch(`${url}/app-id`);
      expect(response.status).toBe(500);
      expect(await response.json()).toEqual({
        name: "CoreRedirectError",
        url: core.url,
        location: `${target.url}/app`,
        message: `the Jazz core at ${core.url} redirected to ${target.url}/app; Jazz never follows a core's redirects, since they would carry the backend credential along: configure durableObjects() with the core's final URL`,
      });
      expect(core.requests).toEqual(["/app"]);
      expect(target.requests).toEqual([]);
    },
  );

  it("keeps the app's data across a restart", async () => {
    const persist = await mkdtemp(join(tmpdir(), "jazz-do-backend-"));
    cleanups.push(() => rm(persist, { recursive: true, force: true }));
    const first = await worker(persist);
    expect((await fetch(`${first.url}/todos`, { method: "POST", body: "kept" })).status).toBe(200);
    await first.dispose();
    const again = await worker(persist);
    expect(await (await fetch(`${again.url}/todos`)).json()).toEqual(["kept"]);
  });

  /** A client of the app through the Worker's `/jazz`, as a device account. */
  async function guestOf(url: string, appId?: string) {
    const session = await createJazzClient({
      ...(appId ? { appId } : {}),
      app,
      permissions: testPermissions(app, {
        format: 1,
        tables: { todos: { read: "true", insert: "true" } },
      } as never),
      serverUrl: `${url}/jazz`,
      driver: { type: "memory" },
      initial: "device",
    } as never);
    cleanups.push(() => session.close());
    return session.getSnapshot().client!;
  }

  it("syncs a device account scoped by the app's id through the Worker's /jazz", async () => {
    const { url } = await worker();
    const guest = await guestOf(url, await (await fetch(`${url}/app-id`)).text());
    await guest.db
      .insert(app.todos, { title: "from a guest", done: false })
      .wait({ tier: "global" });
    // The backend's own routes see the guest's row.
    expect(await (await fetch(`${url}/todos`)).json()).toEqual(["from a guest"]);
  });

  it("syncs a browser-style device account (scoped by the endpoint) through the Worker's /jazz", async () => {
    const { url } = await worker();
    const guest = await guestOf(url);
    await guest.db
      .insert(app.todos, { title: "from a browser", done: false })
      .wait({ tier: "global" });
    expect(await (await fetch(`${url}/todos`)).json()).toEqual(["from a browser"]);
  });

  it("commits a thin client's transaction, which the backend runs on jazz-wasm", async () => {
    const { url } = await worker();
    const appId = await (await fetch(`${url}/app-id`)).text();
    let stored: string | null = null;
    const accounts = await createTestAccountManager({
      appId,
      serverUrl: `${url}/jazz`,
      store: {
        async read() {
          return stored;
        },
        async update(transform) {
          stored = transform(stored);
        },
      },
    });
    const thin = await createThinClient({
      appId,
      serverUrl: `${url}/jazz`,
      account: accounts.createLocalFirst(),
    });
    cleanups.push(() => thin.shutdown());
    const committed = await thin.transaction((tx) => {
      tx.insert(app.todos, { title: "thin one", done: false });
      tx.insert(app.todos, { title: "thin two", done: false });
      return "done";
    });
    expect(committed.value).toBe("done");
    await committed.wait({ tier: "global" });
    expect(await (await fetch(`${url}/todos`)).json()).toEqual(["thin one", "thin two"]);
  });

  describe("refuses", () => {
    it("a backend whose secret isn't the core's, naming it", async () => {
      const { url } = await worker(undefined, { TEST_BACKEND_SECRET: "w".repeat(40) });
      const response = await fetch(`${url}/todos`);
      expect(response.status).toBe(500);
      expect(await response.text()).toMatch(/refused the secret/);
    });

    it("a sync socket from another origin", async () => {
      const { url, miniflare } = await worker();
      const appId = await (await fetch(`${url}/app-id`)).text();
      const response = await miniflare.dispatchFetch(`${url}/jazz/apps/${appId}/ws`, {
        headers: { Upgrade: "websocket", Origin: "https://elsewhere.example" },
      });
      expect(response.status).toBe(403);
      expect(await response.text()).toBe("origin not allowed");
    });

    it("a device account its authenticate doesn't accept: its write never reaches the core", async () => {
      const { url } = await worker(undefined, { TEST_REFUSE_ALL: "1" });
      const guest = await guestOf(url, await (await fetch(`${url}/app-id`)).text());
      const written = guest.db
        .insert(app.todos, { title: "refused guest", done: false })
        .wait({ tier: "global" })
        .then(
          () => "confirmed",
          () => "refused",
        );
      const outcome = await Promise.race([
        written,
        new Promise((resolve) => setTimeout(() => resolve("unconfirmed"), 5_000)),
      ]);
      expect(outcome).not.toBe("confirmed");
      expect(await (await fetch(`${url}/todos`)).json()).toEqual([]);
    });
  });
});

async function discoveryServer(reply: () => Response) {
  const requests: string[] = [];
  const server = createServer(async (req, res) => {
    requests.push(req.url!);
    const response = reply();
    res.writeHead(response.status, Object.fromEntries(response.headers));
    res.end(await response.text());
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  cleanups.push(
    () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections();
        server.close(() => resolve());
      }),
  );
  return { url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, requests };
}
