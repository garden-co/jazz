// Users and local-first guests on a Durable Object core: the app's backend
// (createJazz on durableObjects()) serves its clients over HTTP, and its
// core is the real jazz-wasm host in local workerd. The same checks as
// guest-decision.integration.test.ts runs on the other stores.
import { mkdtemp, rm } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import {
  acceptDeviceAccounts,
  createJazz,
  createJazzClient,
  durableObjects,
  type JazzUser,
} from "../../src/backend/index.js";
import { testApp, testPermissions } from "../../src/testing/index.js";
import { bundleWorker } from "./bundle.js";
import { hostBuilt } from "./host.js";
import { startDurableObjectCore } from "./harness.js";

const app = testApp({
  tables: { todos: { columns: { title: "string", owner: "uuid" } } },
});
const permissionsFile = {
  format: 1,
  tables: { todos: { read: "owner = session.user", insert: "owner = session.user" } },
};
const permissions = testPermissions(app, permissionsFile);

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
  vi.restoreAllMocks();
});

async function listening(server: Server): Promise<number> {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return (server.address() as AddressInfo).port;
}

describe.skipIf(!hostBuilt && !process.env.CI)("clients of a Durable Object core", () => {
  let script: string;
  let persist: string;
  beforeAll(async () => {
    script = await bundleWorker(join(__dirname, "real-core-worker.ts"));
    persist = await mkdtemp(join(tmpdir(), "jazz-do-guests-"));
  });
  afterAll(() => rm(persist, { recursive: true, force: true }));

  /** The app's Jazz on a fresh Durable Object core, served on its own origin. */
  async function appOn(authenticate: (request: Request) => unknown) {
    const core = await startDurableObjectCore({ script });
    cleanups.push(() => core.dispose());
    const jazz = await createJazz({
      app,
      permissions: permissionsFile,
      store: durableObjects({ url: core.url, appId: core.appId, secret: core.secret }),
      authenticate: authenticate as (request: Request) => JazzUser | null,
    }).ready();
    const server = createServer((req, res) =>
      jazz.nodeHandler(req, res, () => {
        res.statusCode = 404;
        res.end();
      }),
    );
    await jazz.attach(server);
    const port = await listening(server);
    cleanups.push(async () => {
      server.closeAllConnections();
      await new Promise((resolve) => server.close(resolve));
      await jazz.close();
    });
    const client = async (initial?: "device") => {
      const session = await createJazzClient({
        appId: jazz.appId,
        app,
        permissions,
        serverUrl: `http://127.0.0.1:${port}${jazz.basePath}`,
        driver: { type: "memory" },
        ...(initial ? { initial } : {}),
      });
      cleanups.push(() => session.close());
      if (!initial) await session.refresh();
      const { client, status, error } = session.getSnapshot();
      if (status !== "ready") throw error ?? new Error(status);
      return client!;
    };
    return { jazz, client, origin: `http://127.0.0.1:${port}` };
  }

  it("syncs a local-first guest the app accepts", async () => {
    const { jazz, client } = await appOn((request) => acceptDeviceAccounts(request));
    const guest = await client("device");
    const row = await guest.db
      .insert(app.todos, { title: "made as a guest", owner: guest.session!.user! })
      .wait({ tier: "global" });
    const seen = await jazz.db.all(app.todos.where({ id: row.id }));
    expect(seen.map((todo) => todo.title)).toEqual(["made as a guest"]);
  });

  it("refuses a local-first guest the app doesn't accept", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const { client } = await appOn(() => null);
    const guest = await client("device");
    void guest.db
      .insert(app.todos, { title: "made as a guest", owner: guest.session!.user! })
      .wait({ tier: "global" })
      .catch(() => undefined);
    await vi.waitFor(
      () =>
        expect(
          warn.mock.calls.some((call) => /refused a local-first guest/.test(call.join(" "))),
        ).toBe(true),
      { timeout: 20_000 },
    );
  });

  it("serves a signed-in user, the session stated by the backend", async () => {
    const { jazz, client, origin } = await appOn((request) => {
      const userId = request.headers.get("x-test-user");
      return userId ? { userId, expiresAt: inAnHour() } : acceptDeviceAccounts(request);
    });
    signInAs(origin, "alice");
    const alice = await client();
    expect(alice.session?.identity).toEqual({ issuer: "jazz:backend", subject: "alice" });
    await alice.db
      .insert(app.todos, { title: "alice's", owner: alice.session!.user! })
      .wait({ tier: "global" });
    const asAlice = await jazz.forReqUser(
      new Request("http://app/x", { headers: { "x-test-user": "alice" } }),
    );
    expect((await asAlice.all(app.todos)).map((todo) => todo.title)).toEqual(["alice's"]);
    expect((await alice.db.all(app.todos, { tier: "remote" })).map((todo) => todo.title)).toEqual([
      "alice's",
    ]);
  });
});

/** The test app's own auth: requests to the app's origin carry `x-test-user`. */
function signInAs(origin: string, userId: string) {
  const realFetch = globalThis.fetch;
  const RealWebSocket = globalThis.WebSocket;
  globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    const request = new Request(input, init);
    if (new URL(request.url).origin === origin) request.headers.set("x-test-user", userId);
    return realFetch(request);
  }) as typeof fetch;
  globalThis.WebSocket = class extends RealWebSocket {
    constructor(url: string | URL, options?: { headers?: Record<string, string> }) {
      // The backend's own sockets to its core keep their headers.
      const headers = new URL(url).host === new URL(origin).host ? { "x-test-user": userId } : {};
      super(url, { ...options, headers: { ...options?.headers, ...headers } } as never);
    }
  } as typeof WebSocket;
  cleanups.push(async () => {
    globalThis.fetch = realFetch;
    globalThis.WebSocket = RealWebSocket;
  });
}

function inAnHour(): number {
  return Math.floor(Date.now() / 1000) + 3600;
}
