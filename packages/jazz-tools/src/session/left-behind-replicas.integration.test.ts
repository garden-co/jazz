import { createServer, type Server } from "node:http";
import type { AddressInfo, Socket } from "node:net";
import { afterEach, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { createJazz, memory } from "../backend/index.js";
import { createDb as createLocalFirstDb } from "../runtime/default-create-db.js";
import { isThinDb } from "../runtime/local-first-upgrade.js";
import type { AccountStore } from "../accounts/persistence.js";
import { createJazzClient } from "./create-jazz-session.js";

// `clearOnSignOut` on a shared browser: a user signed out where the page
// didn't see it (on the server, or another user signing in through the app)
// before a reload must not leave their local replica behind.

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

async function setup() {
  const jazz = await createJazz({
    app,
    permissions,
    store: memory(),
    authenticate: (request: Request) => {
      const userId = request.headers.get("x-test-user");
      return userId ? { userId } : null;
    },
  }).ready();
  cleanups.push(() => jazz.close());
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
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  cleanups.push(
    () =>
      new Promise<void>((resolve) => {
        for (const socket of sockets) socket.destroy();
        server.close(() => resolve());
      }),
  );
  const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;

  // A browser page: the app's cookie names `jar.user`.
  vi.stubGlobal("document", {});
  const storage = new Map<string, string>();
  vi.stubGlobal("localStorage", {
    getItem: (key: string) => storage.get(key) ?? null,
    setItem: (key: string, value: string) => void storage.set(key, value),
    removeItem: (key: string) => void storage.delete(key),
  });
  const jar = { user: "alice" as string | null };
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
        jar.user && new URL(url).host === new URL(origin).host ? { "x-test-user": jar.user } : {};
      super(url, { ...options, headers: { ...options?.headers, ...headers } } as never);
    }
  } as typeof WebSocket;
  cleanups.push(async () => {
    globalThis.fetch = realFetch;
    globalThis.WebSocket = RealWebSocket;
  });

  // The replica a local-first client would keep in this browser's storage:
  // these clients run in memory, and record which user's storage was wiped.
  const wiped: string[] = [];
  const store = memoryStore();
  const open = () =>
    createJazzClient({
      serverUrl: `${origin}/jazz`,
      store,
      clearOnSignOut: true,
      client: async () => ({
        createClient: async (config) => {
          const db = await createLocalFirstDb({ ...config, driver: { type: "memory" } });
          db.deleteClientStorage = async () => {
            wiped.push(config.account.identity.subject);
          };
          return db;
        },
      }),
    });
  return { jar, open, wiped };
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

async function signInLocalFirst(open: () => ReturnType<typeof createJazzClient>) {
  const page = await open();
  await page.loginWithBackend();
  await expect.poll(() => isThinDb(page.getSnapshot().client!.db), { timeout: 15_000 }).toBe(false);
  return page;
}

it("wipes the previous user's replica on the next load once another user signed in", async () => {
  const { jar, open, wiped } = await setup();
  const first = await signInLocalFirst(open);
  // Bob signs in through the app; the page reloads before Jazz noticed.
  jar.user = "bob";
  await first.close();
  expect(wiped).toEqual([]);

  const second = await open();
  cleanups.push(() => second.close());
  await expect.poll(() => wiped, { timeout: 15_000 }).toEqual(["alice"]);
  await expect
    .poll(() => second.getSnapshot().client?.session?.identity.subject, { timeout: 15_000 })
    .toBe("bob");
  await second.close();
}, 60_000);

it("wipes it on the next load after a sign-out on the server", async () => {
  const { jar, open, wiped } = await setup();
  const first = await signInLocalFirst(open);
  jar.user = null;
  await first.close();

  const second = await open();
  cleanups.push(() => second.close());
  await expect.poll(() => wiped, { timeout: 15_000 }).toEqual(["alice"]);
  expect(second.getSnapshot().status).toBe("signed-out");
  await second.close();
}, 60_000);

it("keeps the replica of the user who is still signed in, and wipes once, in the page", async () => {
  const { jar, open, wiped } = await setup();
  const first = await signInLocalFirst(open);
  await first.close();

  const second = await open();
  cleanups.push(() => second.close());
  await expect
    .poll(() => second.getSnapshot().client?.session?.identity.subject, { timeout: 15_000 })
    .toBe("alice");
  expect(wiped).toEqual([]);
  // Signing out in the page wipes it there; the next load has nothing left.
  await second.logout();
  expect(wiped).toEqual(["alice"]);
  jar.user = null;
  await second.close();
  const third = await open();
  cleanups.push(() => third.close());
  await new Promise((resolve) => setTimeout(resolve, 1_000));
  expect(wiped).toEqual(["alice"]);
  await third.close();
}, 60_000);
