// The all-Cloudflare deployment: an app's createJazz backend in a Durable
// Object of the same Worker as its core, on jazz-wasm (no jazz-napi), in
// local workerd. Writes and reads through the app's own routes, and its
// data across a restart.
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import { createJazzClient } from "../../src/backend/index.js";
import { createAccountManager } from "../../src/accounts/create-account-manager.js";
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
    const accounts = await createAccountManager({
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
      const { url } = await worker();
      const appId = await (await fetch(`${url}/app-id`)).text();
      const socket = new (globalThis.WebSocket as unknown as new (
        url: string,
        options: { headers: Record<string, string> },
      ) => WebSocket)(`${url.replace(/^http/, "ws")}/jazz/apps/${appId}/ws`, {
        headers: { origin: "https://elsewhere.example" },
      });
      // The Worker has already upgraded the socket: createJazz's origin
      // check closes it before any session starts (Node refuses the upgrade).
      const closed = await new Promise<{ code: number; reason: string }>((resolve) =>
        socket.addEventListener("close", (event) =>
          resolve({ code: event.code, reason: event.reason }),
        ),
      );
      expect(closed).toEqual({ code: 1008, reason: "origin not allowed" });
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
