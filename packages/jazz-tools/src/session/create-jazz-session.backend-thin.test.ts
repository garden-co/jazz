import { createServer, type Server } from "node:http";
import type { AddressInfo, Socket } from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { createJazz, memory, type Jazz } from "../backend/index.js";
import { createDb as createLocalFirstDb } from "../runtime/default-create-db.js";
import { isThinDb } from "../runtime/local-first-upgrade.js";
import type { AccountStore } from "../accounts/persistence.js";
import { createJazzClient } from "./create-jazz-session.js";
import type { ThinRuntime } from "../thin/thin-runtime.js";
import { decodeThinResponse, isThinResponse } from "../thin/wire.js";

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
  return userId ? { userId, expiresAt: Math.floor(Date.now() / 1000) + 3600 } : null;
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
  return {
    origin: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
    close,
  };
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
      super(url, {
        ...options,
        headers: { ...options?.headers, ...headers },
      } as never);
    }
  } as typeof WebSocket;
  cleanups.push(async () => {
    globalThis.fetch = realFetch;
    globalThis.WebSocket = RealWebSocket;
  });
  return jar;
}

/**
 * Auth by session token (`<user>:<n>`, in the test header): `valid` holds
 * the tokens the app's backend still accepts, so expiring or revoking one
 * takes effect for the sockets that opened with it, as for a JWT cookie.
 */
function tokenAuth(...initial: string[]) {
  const valid = new Set(initial);
  return {
    valid,
    authenticate(request: Request) {
      const token = request.headers.get("x-test-user");
      return token && valid.has(token)
        ? { userId: token.split(":")[0]!, expiresAt: Math.floor(Date.now() / 1000) + 3600 }
        : null;
    },
  };
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
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate,
    }).ready();
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
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate,
    }).ready();
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
    expect(thin.session?.identity).toEqual({
      issuer: "jazz:backend",
      subject: "alice",
    });
    const owner = thin.session!.user!;

    await thin.db.insert(app.todos, { title: "thin", owner }).wait({ tier: "global" });
    expect((await thin.db.all(app.todos)).map((todo) => todo.title)).toEqual(["thin"]);

    await loaderCalled;
    release();
    await expect
      .poll(() => isThinDb(session.getSnapshot().client!.db), {
        timeout: 15_000,
      })
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
    const first = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate,
    }).ready();
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
    const second = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate,
    }).ready();
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
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate,
    }).ready();
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

  // The app's access token expires before it refreshes it (a laptop
  // asleep) and the app ends its session, and a write made
  // meanwhile waits. Once the app signs in again and says so, the same
  // client reconnects and the write lands.
  it("re-authenticates a thin session at its expiresAt without the app seeing a change", async () => {
    let calls = 0;
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: (request) => {
        const token = request.headers.get("x-test-user");
        calls += 1;
        // Each answer holds for 6 s, so the session re-authenticates after 5 s.
        return token ? { userId: token.split(":")[0]!, expiresAt: Date.now() / 1000 + 6 } : null;
      },
    }).ready();
    cleanups.push(() => jazz.close());
    const served = await listen(jazz);
    cookies(served.origin, "alice:1");
    const session = await createJazzClient({
      serverUrl: `${served.origin}/jazz`,
      store: memoryStore(),
      driver: { type: "memory" },
    });
    cleanups.push(() => session.close());
    await session.loginWithBackend();
    const client = session.getSnapshot().client!;
    expect(isThinDb(client.db)).toBe(true);
    const owner = client.session!.user!;
    await client.db.insert(app.todos, { title: "before", owner }).wait({ tier: "global" });
    const before = calls;
    const seen: unknown[] = [];
    const stop = client.db.onSyncStatusChange((status) => seen.push(status.link));
    await expect.poll(() => calls, { timeout: 15_000, interval: 20 }).toBeGreaterThan(before);
    await client.db.insert(app.todos, { title: "after", owner }).wait({ tier: "global" });
    stop();
    expect(seen.filter((link) => (link as { state: string }).state !== "connected")).toEqual([]);
    expect((await jazz.db.all(app.todos)).map((todo) => todo.title).sort()).toEqual([
      "after",
      "before",
    ]);
    await session.close();
  }, 60_000);

  it("lands a write made while the session expired, once the app signs in again", async () => {
    const auth = tokenAuth("alice:1");
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: auth.authenticate,
    }).ready();
    cleanups.push(() => jazz.close());
    const served = await listen(jazz);
    const jar = cookies(served.origin, "alice:1");
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

    // Expired: the backend no longer accepts the token the tab holds, and
    // the app ends the session (its sign-out handler calls revokeUser).
    auth.valid.delete("alice:1");
    await jazz.revokeUser("alice");
    await expect
      .poll(() => client.db.getSyncStatus().link, { timeout: 15_000 })
      .toMatchObject({ state: "offline", reason: "auth" });
    const pending = client.db.insert(app.todos, {
      title: "while expired",
      owner,
    });
    let landed = false;
    void pending.wait({ tier: "global" }).then(() => (landed = true));
    await new Promise((resolve) => setTimeout(resolve, 1_500));
    expect(landed).toBe(false);

    // The app signs in again (a fresh token) and tells Jazz.
    auth.valid.add("alice:2");
    jar.user = "alice:2";
    jar.socketUser = "alice:2";
    await session.reauthenticate();
    const row = await pending.wait({ tier: "global" });
    expect(session.getSnapshot().client).toBe(client);
    expect(client.db.getSyncStatus().link).toEqual({ state: "connected" });
    expect((await jazz.db.all(app.todos.where({ id: row.id }))).map((t) => t.title)).toEqual([
      "while expired",
    ]);
    await session.close();
  }, 60_000);

  // R29 as reported: the write is on the wire when the token expires, and
  // the app ends the session before its answer arrives. The
  // refusal is final for that credential, so the thin client doesn't send
  // it again: the write ends with its fate unknown (it may have committed,
  // and here it did), and the app signs in again without writing it twice.
  it("ends a write in flight as the session expires with its fate unknown, and doesn't write it twice", async () => {
    const auth = tokenAuth("alice:1");
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: auth.authenticate,
    }).ready();
    cleanups.push(() => jazz.close());
    const served = await listen(jazz);
    const jar = cookies(served.origin, "alice:1");
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

    // The server's answer to the next write never reaches the client.
    const runtime = client.db.currentRuntime() as ThinRuntime;
    const internals = runtime as unknown as {
      oneShots: Map<number, { op: { type: string }; sent: boolean }>;
      receive(carrier: unknown, piece: Uint8Array): void;
    };
    const receive = internals.receive.bind(runtime);
    let held: number | undefined;
    let dropped = 0;
    internals.receive = (carrier, piece) => {
      if (held !== undefined && isThinResponse(piece) && decodeThinResponse(piece).id === held) {
        dropped++;
        return;
      }
      receive(carrier, piece);
    };
    cleanups.push(async () => void (internals.receive = receive));
    const pending = client.db.insert(app.todos, {
      title: "as it expired",
      owner,
    });
    const landed = pending.wait({ tier: "global" }).catch((error: unknown) => error);
    const [id] = [...internals.oneShots].find(([, oneShot]) => oneShot.op.type === "insert")!;
    held = id;
    await expect.poll(() => dropped).toBe(1);
    expect(internals.oneShots.has(id)).toBe(true);

    // Then the token expires, and the app ends the session.
    auth.valid.delete("alice:1");
    await jazz.revokeUser("alice");
    await expect
      .poll(() => client.db.getSyncStatus().link, { timeout: 15_000 })
      .toMatchObject({ state: "offline", reason: "auth" });

    auth.valid.add("alice:2");
    jar.user = jar.socketUser = "alice:2";
    expect(await landed).toMatchObject({ name: "ThinConnectionLostError" });
    held = undefined;
    await session.reauthenticate();
    expect(session.getSnapshot().client).toBe(client);
    await expect
      .poll(() => client.db.getSyncStatus().link, { timeout: 15_000 })
      .toEqual({ state: "connected" });
    expect(
      (await jazz.db.all(app.todos)).filter((todo) => todo.title === "as it expired"),
    ).toHaveLength(1);
    await session.close();
  }, 60_000);

  // The app revokes a user whose tab stays open and in view: the backend's
  // revokeUser ends the socket, and the tab signs out with no other app code.
  it("signs a revoked user's open tab out", async () => {
    const auth = tokenAuth("alice:1");
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: auth.authenticate,
    }).ready();
    cleanups.push(() => jazz.close());
    const served = await listen(jazz);
    cookies(served.origin, "alice:1");
    vi.stubGlobal("document", { visibilityState: "visible" });
    const session = await createJazzClient({
      serverUrl: `${served.origin}/jazz`,
      store: memoryStore(),
      driver: { type: "memory" },
    });
    cleanups.push(() => session.close());
    await session.loginWithBackend();
    const client = session.getSnapshot().client!;
    const owner = client.session!.user!;
    await client.db.insert(app.todos, { title: "private", owner }).wait({ tier: "global" });

    auth.valid.delete("alice:1");
    await jazz.revokeUser("alice");
    const revokedAt = Date.now();
    await expect
      .poll(() => session.getSnapshot().status, {
        timeout: 70_000,
        interval: 250,
      })
      .toBe("signed-out");
    // The refusal reaches the watch within the re-check; it confirms after
    // its grace for a cookie the app is refreshing (30s), then once more.
    // Slow by design: it waits out that grace, with room for a loaded runner.
    expect(Date.now() - revokedAt).toBeLessThan(50_000);
  }, 90_000);

  // Node, Deno, a CLI: no cookie carries the backend's session on the sync
  // connection, so a backend sign-in would report ready and never sync.
  it("refuses a backend sign-in on a host that sends no cookies, before any request", async () => {
    const requests: string[] = [];
    const session = await createJazzClient({
      serverUrl: "http://127.0.0.1:9/jazz",
      store: memoryStore(),
      driver: { type: "memory" },
      fetch: async (input) => {
        requests.push(String(input));
        return new Response(null, { status: 500 });
      },
    });
    cleanups.push(() => session.close());

    const refusal = /need a browser.*jazz-tools\/react-native or jazz-tools\/expo/;
    await expect(session.loginWithBackend()).rejects.toThrow(refusal);
    expect(requests).toEqual([]);
    expect(session.getSnapshot().status).not.toBe("ready");

    // A guest can't sign up through the backend here either.
    const guest = await createJazzClient({
      serverUrl: "http://127.0.0.1:9/jazz",
      store: memoryStore(),
      driver: { type: "memory" },
      initial: "local-first",
      client: async () => ({ createClient: createLocalFirstDb }),
      fetch: async (input) => {
        requests.push(String(input));
        return new Response(null, { status: 500 });
      },
    });
    cleanups.push(() => guest.close());
    const account = guest.getSnapshot().account;
    await expect(guest.linkWithBackend()).rejects.toThrow(refusal);
    expect(requests).toEqual([]);
    expect(guest.getSnapshot().account).toBe(account);
  });
});

describe("another user signing in on a local-first browser", () => {
  // Bob signs in at the app's backend while Alice's local-first client is
  // open (no sign-out first): her socket now carries his cookie, so the
  // server refuses her client, and the session must still switch to him.
  async function aliceLocalFirst() {
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate,
    }).ready();
    cleanups.push(() => jazz.close());
    const served = await listen(jazz);
    const jar = cookies(served.origin, "alice");
    const session = await createJazzClient({
      serverUrl: `${served.origin}/jazz`,
      store: memoryStore(),
      driver: { type: "memory" },
      client: async () => ({ createClient: createLocalFirstDb }),
    });
    cleanups.push(() => session.close());
    await session.loginWithBackend();
    await expect
      .poll(() => isThinDb(session.getSnapshot().client!.db), {
        timeout: 15_000,
      })
      .toBe(false);
    const alice = session.getSnapshot().client!;
    const owner = alice.session!.user!;
    await alice.db.insert(app.todos, { title: "alice's", owner }).wait({ tier: "global" });
    return { jazz, served, jar, session, alice, owner };
  }

  // A smoke test: Alice left nothing behind, so her sign-out goes through
  // either way. The variant below, with a write left behind, is R48's.
  it("switches to the new user", async () => {
    const { jar, session, alice } = await aliceLocalFirst();
    const warned = vi.spyOn(console, "warn").mockImplementation(() => {});
    cleanups.push(async () => warned.mockRestore());

    jar.user = jar.socketUser = "bob";
    await session.reauthenticate();
    const snapshot = session.getSnapshot();
    expect(snapshot.status).toBe("ready");
    expect(snapshot.client?.session?.identity).toEqual({
      issuer: "jazz:backend",
      subject: "bob",
    });
    expect(snapshot.client?.session?.user).not.toBe(alice.session!.user);
    // Nothing of Alice's was left behind, so there is nothing to warn about.
    expect(warned).not.toHaveBeenCalled();
  }, 60_000);

  it("switches past the previous user's write the server won't take, and says so", async () => {
    const { jazz, served, jar, session, owner } = await aliceLocalFirst();
    const warned = vi.spyOn(console, "warn").mockImplementation(() => {});
    cleanups.push(async () => warned.mockRestore());

    // Alice writes while the backend is away; Bob signs in when it is back.
    await served.close();
    session.getSnapshot().client!.db.insert(app.todos, { title: "unsynced", owner });
    jar.user = jar.socketUser = "bob";
    await listen(jazz, Number(new URL(served.origin).port));
    await session.reauthenticate();
    const snapshot = session.getSnapshot();
    expect(snapshot.status).toBe("ready");
    expect(snapshot.client?.session?.identity).toEqual({
      issuer: "jazz:backend",
      subject: "bob",
    });
    expect(warned).toHaveBeenCalledWith(
      expect.stringMatching(/dropped 1 write\(s\) the server refused/),
      expect.anything(),
    );
  }, 60_000);
});

describe("a shared device (clearOnSignOut)", () => {
  // Kim starts as a guest, signs up (the backend links her guest), and signs
  // out. Nothing left on the device may reopen her account: not her replica,
  // and not her guest's signing root either.
  it("leaves no root that reopens a guest who signed up and signed out", async () => {
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate,
    }).ready();
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz);
    // A browser page, no one signed in at the backend yet.
    const jar = cookies(origin, null);
    let stored: string | null = null;
    const store: AccountStore = {
      read: async () => stored,
      update: async (transform) => void (stored = transform(stored)),
    };
    const session = await createJazzClient({
      serverUrl: `${origin}/jazz`,
      store,
      driver: { type: "memory" },
      initial: "local-first",
      clearOnSignOut: true,
      client: async () => ({ createClient: createLocalFirstDb }),
    });
    cleanups.push(() => session.close());
    const guest = session.getSnapshot().client!;
    const account = guest.session!.user!;
    const [root] = JSON.parse(stored!).roots;
    expect(root).toEqual(expect.any(String));

    // The app's sign-up created "kim"; the backend now names her.
    jar.user = jar.socketUser = "kim";
    await session.linkWithBackend();
    expect(session.getSnapshot().client!.session!.user).toBe(account);
    await session
      .getSnapshot()
      .client!.db.insert(app.todos, {
        title: "kim-private-note",
        owner: account,
      })
      .wait({ tier: "global" });
    jar.user = jar.socketUser = null;
    await session.logout();
    expect(session.getSnapshot().status).toBe("signed-out");
    expect(JSON.parse(stored!)).toMatchObject({ roots: [], selected: null });
    await session.close();

    // The next person on the device continues as a guest of their own, and
    // nothing on the device holds Kim's root to reopen her with.
    const next = await createJazzClient({
      serverUrl: `${origin}/jazz`,
      store,
      driver: { type: "memory" },
      initial: "local-first",
      clearOnSignOut: true,
      client: async () => ({ createClient: createLocalFirstDb }),
    });
    cleanups.push(() => next.close());
    expect(next.getSnapshot().client!.session!.user).not.toBe(account);
    expect(stored).not.toContain(root);
  }, 60_000);
});

describe("signing out with writes the server won't take", () => {
  // The backend's session ended (cookies cleared, expired) with a write still
  // on this device: the sign-out must go through, or the app is stuck on an
  // error it can't leave, through reloads too.
  it("signs out when the backend does, past a write the server won't take", async () => {
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate,
    }).ready();
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
      .poll(() => isThinDb(session.getSnapshot().client!.db), {
        timeout: 15_000,
      })
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
    await session.reauthenticate();
    expect(session.getSnapshot().status).toBe("signed-out");
    // In memory, it says so: those writes are gone.
    expect(warned).toHaveBeenCalledWith(
      expect.stringMatching(/dropped 1 write\(s\) the server refused/),
      expect.anything(),
    );
    await session.close();
  }, 60_000);
});
