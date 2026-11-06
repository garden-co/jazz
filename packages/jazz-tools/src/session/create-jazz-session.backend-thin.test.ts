import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { createJazz, memory, type Jazz } from "../backend/index.js";
import { createDb as createLocalFirstDb } from "../runtime/default-create-db.js";
import { isThinDb } from "../runtime/local-first-upgrade.js";
import type { AccountStore } from "../accounts/persistence.js";
import { createJazzClient } from "./create-jazz-session.js";

// The browser session against an app's own backend (`createJazz`), which
// signs users in from their cookies: the hero examples' setup.

const app = s.defineApp(
  { todos: s.table({ title: s.string(), owner: s.uuid() }, {}) },
  { migrations: "none" },
);
const permissions = s.definePermissions(app, ({ policy, session }) => {
  policy.todos.allowRead.where({ owner: session.user });
  policy.todos.allowInsert.where({ owner: session.user });
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

async function serve(jazz: Jazz, port = 0): Promise<string> {
  const server: Server = createServer((req, res) =>
    jazz.nodeHandler(req, res, () => {
      res.statusCode = 404;
      res.end();
    }),
  );
  await jazz.attach(server);
  await new Promise<void>((resolve) => server.listen(port, "127.0.0.1", resolve));
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
  // A browser page: the host that sends the backend's cookie.
  vi.stubGlobal("document", {});
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
  // A returning user has no account stored on this device: the backend names
  // them. Until it has answered, the session must not say "signed-out", or
  // every app shows its sign-in screen for a moment on each page load.
  it("opens a signed-in user's session without reporting signed-out first", async () => {
    const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz);
    signInAs(origin, "alice");

    const session = await createJazzClient({
      serverUrl: `${origin}/jazz`,
      store: memoryStore(),
      driver: { type: "memory" },
    });
    cleanups.push(() => session.close());

    const snapshot = session.getSnapshot();
    expect(snapshot.status).toBe("ready");
    expect(snapshot.client?.session?.identity).toEqual({
      issuer: "jazz:backend",
      subject: "alice",
    });
  }, 30_000);

  it("opens signed out once the backend says no one is signed in", async () => {
    const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz);

    const session = await createJazzClient({
      serverUrl: `${origin}/jazz`,
      store: memoryStore(),
      driver: { type: "memory" },
    });
    cleanups.push(() => session.close());

    expect(session.getSnapshot().status).toBe("signed-out");
  }, 30_000);

  it("opens signed out when the backend can't be reached", async () => {
    // Nothing listens here: the first check fails fast and the session opens
    // as before rather than waiting on it.
    const session = await createJazzClient({
      serverUrl: "http://127.0.0.1:9/jazz",
      store: memoryStore(),
      driver: { type: "memory" },
    });
    cleanups.push(() => session.close());

    expect(session.getSnapshot().status).toBe("signed-out");
  }, 30_000);

  it("signs in on the thin client and upgrades with a client loader", async () => {
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate,
    }).ready();
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz);
    signInAs(origin, "alice");

    let upgrade!: () => void;
    const loaderCalled = new Promise<void>((resolve) => (upgrade = resolve));
    let release!: () => void;
    const released = new Promise<void>((resolve) => (release = resolve));
    const session = await createJazzClient({
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
    expect(thin.session?.identity).toEqual({ issuer: "jazz:backend", subject: "alice" });
    const owner = thin.session!.user!;

    await thin.db.insert(app.todos, { title: "thin", owner }).wait({ tier: "global" });
    expect((await thin.db.all(app.todos)).map((todo) => todo.title)).toEqual(["thin"]);

    await loaderCalled;
    release();
    await expect
      .poll(() => isThinDb(session.getSnapshot().client!.db), { timeout: 15_000 })
      .toBe(false);
    const local = session.getSnapshot().client!;
    expect(local.session?.user).toBe(owner);
    await local.db.insert(app.todos, { title: "local", owner }).wait({ tier: "global" });
    expect((await local.db.all(app.todos)).map((todo) => todo.title).sort()).toEqual([
      "local",
      "thin",
    ]);
  }, 60_000);

  // The core no longer holds the user's account (a reset dev store, a new
  // core): the sync server refuses the thin client, which must say so for the
  // session to sign the user in again instead of hanging.
  it("signs a user in again after the backend restarts on an empty store", async () => {
    const first = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    const closeFirst = once(() => first.close());
    cleanups.push(closeFirst);
    const origin = await serve(first);
    signInAs(origin, "alice");
    const session = await createJazzClient({
      serverUrl: `${origin}/jazz`,
      store: memoryStore(),
      driver: { type: "memory" },
    });
    cleanups.push(() => session.close());
    await session.loginWithBackend();
    const before = session.getSnapshot().client!;
    const lost = before.session!.user!;
    await before.db.insert(app.todos, { title: "before", owner: lost }).wait({ tier: "global" });
    // The thin client reports its link: the backend watch acts on a refusal.
    expect(before.db.getSyncStatus().link.state).toBe("connected");

    // The same backend, restarted on an empty store: alice has no account.
    const servers = cleanups.splice(cleanups.length - 3, 1);
    await closeFirst();
    await servers[0]!();
    const second = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    cleanups.push(() => second.close());
    await serve(second, Number(new URL(origin).port));

    await expect
      .poll(() => session.getSnapshot().client?.session?.user, { timeout: 30_000 })
      .not.toBe(lost);
    const after = session.getSnapshot().client!;
    const row = await after.db
      .insert(app.todos, { title: "after", owner: after.session!.user! })
      .wait({ tier: "global" });
    expect((await second.db.all(app.todos.where({ id: row.id }))).map((t) => t.title)).toEqual([
      "after",
    ]);
    // Its sockets close before the server does.
    await session.close();
  }, 60_000);
});

function once(run: () => Promise<unknown>): () => Promise<unknown> {
  let done: Promise<unknown> | undefined;
  return () => (done ??= run());
}
