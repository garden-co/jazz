import { createServer, type Server } from "node:http";
import type { AddressInfo, Socket } from "node:net";
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
  return (await listen(jazz, port)).origin;
}

/** Serve `jazz`; `close` ends every connection, upgraded sockets too. */
async function listen(jazz: Jazz, port = 0) {
  const server: Server = createServer((req, res) =>
    jazz.nodeHandler(req, res, () => {
      res.statusCode = 404;
      res.end();
    }),
  );
  const sockets = new Set<Socket>();
  server.on("connection", (socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
  });
  await jazz.attach(server);
  await new Promise<void>((resolve) => server.listen(port, "127.0.0.1", resolve));
  let closing: Promise<void> | undefined;
  const close = () =>
    (closing ??= new Promise<void>((resolve) => {
      for (const socket of sockets) socket.destroy();
      server.close(() => resolve());
    }));
  cleanups.push(close);
  return { origin: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, close };
}

/** Every request and socket to `origin` carries `userId`'s session, as a cookie would. */
function signInAs(origin: string, userId: string) {
  return cookies(origin, userId);
}

/**
 * Requests and sockets to `origin` carry the signed-in user's session, as a
 * cookie would; `socketUser` sets what sockets carry alone (a stale cookie).
 */
function cookies(origin: string, userId: string | null) {
  // A browser page: the host that sends the backend's cookie.
  vi.stubGlobal("document", {});
  const jar = { user: userId, socketUser: userId };
  const realFetch = globalThis.fetch;
  const RealWebSocket = globalThis.WebSocket;
  globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    const request = new Request(input, init);
    if (jar.user && new URL(request.url).origin === origin)
      request.headers.set("x-test-user", jar.user);
    return realFetch(request);
  }) as typeof fetch;
  globalThis.WebSocket = class extends RealWebSocket {
    constructor(url: string | URL, options?: { headers?: Record<string, string> }) {
      const headers =
        jar.socketUser && new URL(url).host === new URL(origin).host
          ? { "x-test-user": jar.socketUser }
          : {};
      super(url, { ...options, headers: { ...options?.headers, ...headers } } as never);
    }
  } as typeof WebSocket;
  cleanups.push(async () => {
    globalThis.fetch = realFetch;
    globalThis.WebSocket = RealWebSocket;
  });
  return jar;
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
    cleanups.push(() => first.close());
    const served = await listen(first);
    signInAs(served.origin, "alice");
    const session = await createJazzClient({
      serverUrl: `${served.origin}/jazz`,
      store: memoryStore(),
      driver: { type: "memory" },
    });
    cleanups.push(() => session.close());
    await session.loginWithBackend();
    const before = session.getSnapshot().client!;
    const lost = before.session!.user!;
    await before.db.insert(app.todos, { title: "before", owner: lost }).wait({ tier: "global" });
    expect(before.db.getSyncStatus().link).toEqual({ state: "connected" });
    const links: string[] = [];
    before.db.onSyncStatusChange(({ link }) =>
      links.push(link.state === "offline" ? `offline ${link.reason}` : link.state),
    );

    // The same backend, restarted on an empty store: alice has no account.
    await first.close();
    await served.close();
    const second = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    cleanups.push(() => second.close());
    await listen(second, Number(new URL(served.origin).port));

    const signedInAgain = () => {
      const { status, client } = session.getSnapshot();
      return status === "ready" && client?.session?.user !== lost ? client : undefined;
    };
    await expect.poll(signedInAgain, { timeout: 30_000 }).toBeDefined();
    // The refusal is what told the session.
    expect(links).toContain("offline auth");
    const after = signedInAgain()!;
    const row = await after.db
      .insert(app.todos, { title: "after", owner: after.session!.user! })
      .wait({ tier: "global" });
    expect((await second.db.all(app.todos.where({ id: row.id }))).map((t) => t.title)).toEqual([
      "after",
    ]);
    // Its sockets close before the server does.
    await session.close();
  }, 60_000);

  // A socket refused for a stale cookie while the backend still names the
  // user: the same client reconnects with fresh cookies and reports it.
  it("reports connected again when a refused client reconnects", async () => {
    const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    cleanups.push(() => jazz.close());
    const served = await listen(jazz);
    const jar = cookies(served.origin, "alice");
    const session = await createJazzClient({
      serverUrl: `${served.origin}/jazz`,
      store: memoryStore(),
      driver: { type: "memory" },
    });
    cleanups.push(() => session.close());
    await session.loginWithBackend();
    const client = session.getSnapshot().client!;
    const owner = client.session!.user!;
    await client.db.insert(app.todos, { title: "first", owner }).wait({ tier: "global" });

    // The next socket goes without the cookie and is refused.
    jar.socketUser = null;
    await client.db.disconnect();
    await client.db.reconnect();
    await expect
      .poll(() => client.db.getSyncStatus().link, { timeout: 15_000 })
      .toMatchObject({ state: "offline", reason: "auth" });

    // Fresh cookies: the watch's reconnect binds again.
    jar.socketUser = "alice";
    await expect
      .poll(() => client.db.getSyncStatus().link, { timeout: 30_000 })
      .toEqual({ state: "connected" });
    expect(session.getSnapshot().client).toBe(client);
    await client.db.insert(app.todos, { title: "second", owner }).wait({ tier: "global" });
    await session.close();
  }, 60_000);
});

describe("signing out with writes the server won't take", () => {
  // The backend's session ended (cookies cleared, expired) with a write still
  // on this device: the sign-out must go through, or the app is stuck on an
  // error it can't leave, through reloads too.
  it("signs out when the backend does, past a write the server won't take", async () => {
    const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    cleanups.push(() => jazz.close());
    const served = await listen(jazz);
    const { origin } = served;
    const jar = cookies(origin, "alice");
    const session = await createJazzClient({
      serverUrl: `${origin}/jazz`,
      store: memoryStore(),
      driver: { type: "memory" },
      client: async () => ({ createClient: createLocalFirstDb }),
    });
    cleanups.push(() => session.close());
    await session.loginWithBackend();
    await expect
      .poll(() => isThinDb(session.getSnapshot().client!.db), { timeout: 15_000 })
      .toBe(false);
    const local = session.getSnapshot().client!;
    const owner = local.session!.user!;
    await local.db.insert(app.todos, { title: "synced", owner }).wait({ tier: "global" });

    // The backend goes down; a write stays on this device; the cookie goes.
    await served.close();
    await local.db.insert(app.todos, { title: "unsynced", owner });
    jar.user = jar.socketUser = null;
    await listen(jazz, Number(new URL(origin).port));
    const warned = vi.spyOn(console, "warn").mockImplementation(() => {});
    cleanups.push(async () => warned.mockRestore());
    await (session as unknown as { reauthenticate(): Promise<void> }).reauthenticate();
    expect(session.getSnapshot().status).toBe("signed-out");
    // In memory, it says so: those writes are gone.
    expect(warned).toHaveBeenCalledWith(
      expect.stringMatching(/dropped 1 write\(s\) the server refused/),
      expect.anything(),
    );
    await session.close();
  }, 60_000);
});
