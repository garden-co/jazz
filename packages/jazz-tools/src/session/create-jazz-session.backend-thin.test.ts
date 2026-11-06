import { randomUUID } from "node:crypto";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { createJazz, memory, type Jazz } from "../backend/index.js";
import { createDb as createLocalFirstDb } from "../runtime/default-create-db.js";
import { isThinDb } from "../runtime/local-first-upgrade.js";
import type { AccountStore } from "../accounts/persistence.js";
import { createJazzSession } from "./create-jazz-session.js";

// The browser session against an app's own backend (`createJazz`), which
// signs users in from their cookies: the hero examples' setup.

const app = s.defineApp(
  { todos: s.table({ title: s.string(), owner: s.uuid() }, {}) },
  { migrations: "none" },
);
const permissions = s.definePermissions(app, ({ policy, session }) => {
  policy.todos.allowRead.where({ owner: session.user.account });
  policy.todos.allowInsert.where({ owner: session.user.account });
});

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
  vi.unstubAllGlobals();
});

/** The test app's own auth: the user named in a header, standing in for a cookie. */
function authenticate(request: Request) {
  const userId = request.headers.get("x-test-user");
  return userId ? { userId } : null;
}

async function serve(jazz: Jazz): Promise<string> {
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
  return `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
}

/** Every request and socket to `origin` carries `userId`'s session, as a cookie would. */
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
      const headers = new URL(url).host === new URL(origin).host ? { "x-test-user": userId } : {};
      super(url, { ...options, headers: { ...options?.headers, ...headers } } as never);
    }
  } as typeof WebSocket;
  cleanups.push(async () => {
    globalThis.fetch = realFetch;
    globalThis.WebSocket = RealWebSocket;
  });
}

function memoryStore(): AccountStore {
  let stored: string | null = null;
  return {
    async read() {
      return stored;
    },
    async update(transform) {
      stored = transform(stored);
    },
  };
}

describe("browser session against the app's backend", () => {
  it("signs in on the thin client and upgrades with a client loader", async () => {
    const appId = randomUUID();
    const jazz = await createJazz({ app, permissions, store: memory({ appId }), authenticate });
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz);
    signInAs(origin, "alice");

    let upgrade!: () => void;
    const loaderCalled = new Promise<void>((resolve) => (upgrade = resolve));
    let release!: () => void;
    const released = new Promise<void>((resolve) => (release = resolve));
    const session = await createJazzSession({
      serverUrl: `${origin}/jazz`,
      store: memoryStore(),
      driver: { type: "memory" },
      async client() {
        upgrade();
        await released;
        return { createClient: createLocalFirstDb };
      },
    });
    cleanups.push(() => session.close());
    await session.loginWithBackend();
    const thin = session.getSnapshot().client!;
    expect(isThinDb(thin.db)).toBe(true);
    expect(thin.session?.user?.identity).toEqual({ issuer: `jazz:app:${appId}`, subject: "alice" });
    const owner = thin.session!.user!.account;

    await thin.db.insert(app.todos, { title: "thin", owner }).wait({ tier: "global" });
    expect((await thin.db.all(app.todos)).map((todo) => todo.title)).toEqual(["thin"]);

    await loaderCalled;
    release();
    await expect
      .poll(() => isThinDb(session.getSnapshot().client!.db), { timeout: 15_000 })
      .toBe(false);
    const local = session.getSnapshot().client!;
    expect(local.session?.user?.account).toBe(owner);
    await local.db.insert(app.todos, { title: "local", owner }).wait({ tier: "global" });
    expect((await local.db.all(app.todos)).map((todo) => todo.title).sort()).toEqual([
      "local",
      "thin",
    ]);
  }, 60_000);
});
