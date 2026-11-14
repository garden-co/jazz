import { createServer, type Server } from "node:http";
import type { AddressInfo, Socket } from "node:net";
import { afterEach, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { acceptDeviceAccounts, createJazz, memory } from "../backend/index.js";
import { createDb as createLocalFirstDb } from "../runtime/default-create-db.js";
import { isThinDb } from "../runtime/local-first-upgrade.js";
import type { AccountStore } from "../accounts/persistence.js";
import { openBrowserSessionCore as createJazzClient } from "./create-jazz-session.js";

// `clearOnSignOut` on a shared browser, through a real session and backend
// (#5250): the last tab out wipes, a user signed out where no page saw it
// (on the server, or another user signing in through the app) doesn't leave
// their local replica behind, and a still-signed-in tab keeps its data.

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

async function setup(options: { guests?: boolean } = {}) {
  const jazz = await createJazz({
    app,
    permissions,
    store: memory(),
    authenticate: (request: Request) => {
      const userId = request.headers.get("x-test-user");
      if (userId) return { userId, expiresAt: Math.floor(Date.now() / 1000) + 3600 };
      return options.guests ? acceptDeviceAccounts(request) : null;
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
  const page = Object.assign(new EventTarget(), { visibilityState: "visible" });
  vi.stubGlobal("document", page);
  const storage = new Map<string, string>();
  vi.stubGlobal("localStorage", {
    getItem: (key: string) => storage.get(key) ?? null,
    setItem: (key: string, value: string) => void storage.set(key, value),
    removeItem: (key: string) => void storage.delete(key),
    key: (index: number) => [...storage.keys()][index] ?? null,
    get length() {
      return storage.size;
    },
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
  const subjects = new Map<string, string>();
  const hold = {
    // Holds a wipe's delete until released (a slow storage teardown).
    wipe: undefined as Promise<void> | undefined,
    deleting: false,
    // Fails the next delete (storage the browser won't delete).
    fail: false,
  };
  const store = memoryStore();
  const open = () =>
    createJazzClient({
      serverUrl: `${origin}/jazz`,
      store,
      clearOnSignOut: true,
      client: async () => ({
        createClient: async (config) => {
          subjects.set(config.account.id, config.account.identity.subject);
          return createLocalFirstDb({ ...config, driver: { type: "memory" } });
        },
        deleteLocalFirstStore: async (config) => {
          hold.deleting = true;
          try {
            await hold.wipe;
            if (hold.fail) {
              hold.fail = false;
              throw new Error("IndexedDB deletion failed");
            }
          } finally {
            hold.deleting = false;
          }
          wiped.push(subjects.get(config.accountId) ?? config.accountId);
          return "deleted" as const;
        },
      }),
    });
  return { jar, open, wiped, hold, storage, page, store };
}

/** Web Locks as a browser has them: every page of the test shares them. */
function stubWebLocks() {
  const held = new Map<string, number>();
  vi.stubGlobal("navigator", {
    locks: {
      async query() {
        return {
          held: [...held]
            .filter(([, n]) => n !== 0)
            .map(([name, n]) => ({ name, mode: n < 0 ? "exclusive" : "shared" })),
        };
      },
      async request(
        name: string,
        options: { mode: "shared" | "exclusive"; ifAvailable?: boolean },
        callback: (lock: unknown) => Promise<unknown>,
      ) {
        if (options.mode === "exclusive") {
          if (held.get(name)) return callback(null);
          held.set(name, -1);
        } else {
          // As in a browser, a shared hold waits out an exclusive one.
          while (held.get(name) === -1) await new Promise((resolve) => setTimeout(resolve, 10));
          held.set(name, (held.get(name) ?? 0) + 1);
        }
        try {
          return await callback({ name });
        } finally {
          if (options.mode === "exclusive") held.delete(name);
          else held.set(name, held.get(name)! - 1);
        }
      },
    },
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

it("keeps the replica on one signed-out answer the next contradicts (a failed refresh)", async () => {
  const { jar, open, wiped } = await setup();
  const first = await signInLocalFirst(open);
  await first.close();
  // One answer says no one (the app's session refresh failed once); the next names alice again.
  jar.user = null;
  const second = await open();
  cleanups.push(() => second.close());
  jar.user = "alice";
  await new Promise((resolve) => setTimeout(resolve, 3_000));
  expect(wiped).toEqual([]);
  await second.close();
}, 60_000);

it("lets a user who signs back in during the wipe open a fresh replica, still recorded", async () => {
  const { jar, open, wiped, hold, storage } = await setup();
  const first = await signInLocalFirst(open);
  jar.user = null;
  await first.close();

  let finish!: () => void;
  hold.wipe = new Promise<void>((resolve) => (finish = resolve));
  const second = await open();
  cleanups.push(() => second.close());
  // The wipe has started (after its confirming answer); alice signs in meanwhile.
  await new Promise((resolve) => setTimeout(resolve, 3_000));
  jar.user = "alice";
  const signingIn = second.loginWithBackend();
  await new Promise((resolve) => setTimeout(resolve, 500));
  expect(wiped).toEqual([]);
  finish();
  await signingIn;
  expect(wiped).toEqual(["alice"]);
  await expect
    .poll(() => second.getSnapshot().client?.session?.identity.subject, { timeout: 15_000 })
    .toBe("alice");
  // Her new replica is recorded, for a later missed sign-out.
  const recorded = [...storage].find(([key]) => key.startsWith("jazz-replica-v2:"));
  expect(recorded?.[1]).toContain('"subject":"alice"');
  await second.close();
}, 60_000);

it("finishes a wipe whose delete failed before the user's next sign-in opens their replica", async () => {
  const { jar, open, wiped, hold, storage } = await setup();
  const first = await signInLocalFirst(open);
  jar.user = null;
  await first.close();

  hold.fail = true;
  const warned = vi.spyOn(console, "warn").mockImplementation(() => {});
  cleanups.push(async () => warned.mockRestore());
  const second = await open();
  cleanups.push(() => second.close());
  await expect
    .poll(() => warned.mock.calls.some(([message]) => /tries again later/.test(String(message))), {
      timeout: 15_000,
    })
    .toBe(true);
  expect(wiped).toEqual([]);
  const entry = () => [...storage].find(([key]) => key.startsWith("jazz-replica-v2:"))?.[1] ?? "";
  expect(entry()).toContain('"wiping":true');
  // Alice signs back in: her half-deleted replica is finished first, then opened fresh.
  jar.user = "alice";
  await second.loginWithBackend();
  expect(wiped).toEqual(["alice"]);
  expect(second.getSnapshot().client?.session?.identity.subject).toBe("alice");
  expect(entry()).toContain('"subject":"alice"');
  expect(entry()).not.toContain("wiping");
}, 60_000);

it("holds the user's sign-in until a delete that began finishes, however late", async () => {
  stubWebLocks();
  const { jar, open, wiped, hold, storage } = await setup();
  const first = await signInLocalFirst(open);
  jar.user = null;
  await first.close();

  let finish!: () => void;
  hold.wipe = new Promise<void>((resolve) => (finish = resolve));
  const second = await open();
  cleanups.push(() => second.close());
  await expect.poll(() => hold.deleting, { timeout: 15_000 }).toBe(true);
  jar.user = "alice";
  let signedIn = false;
  const signingIn = second.loginWithBackend().then(() => (signedIn = true));
  await new Promise((resolve) => setTimeout(resolve, 1_000));
  expect(signedIn).toBe(false);
  finish();
  await signingIn;
  expect(wiped).toEqual(["alice"]);
  await expect
    .poll(() => second.getSnapshot().client?.session?.identity.subject, { timeout: 15_000 })
    .toBe("alice");
  const recorded = [...storage].find(([key]) => key.startsWith("jazz-replica-v2:"));
  expect(recorded?.[1]).toContain('"subject":"alice"');
}, 60_000);

it("wipes nothing once the session is closed", async () => {
  const { jar, open, wiped } = await setup();
  const first = await signInLocalFirst(open);
  jar.user = null;
  await first.close();

  // Closed while it confirms the signed-out answer.
  const second = await open();
  await second.close();
  await new Promise((resolve) => setTimeout(resolve, 4_000));
  expect(wiped).toEqual([]);
}, 60_000);

it("records a guest's replica as a device account, and keeps it while the device selects that guest", async () => {
  const { jar, open, storage, wiped } = await setup({ guests: true });
  jar.user = null;
  const page = await open();
  cleanups.push(() => page.close());
  await page.createLocalFirst();
  await expect.poll(() => isThinDb(page.getSnapshot().client!.db), { timeout: 15_000 }).toBe(false);
  const entry = [...storage].find(([key]) => key.startsWith("jazz-replica-v2:"))?.[1] ?? "";
  expect(entry).toContain('"kind":"device"');
  // Its tab closes without signing out: the guest is still the device's, and so is its data.
  await page.close();
  const next = await open();
  cleanups.push(() => next.close());
  await new Promise((resolve) => setTimeout(resolve, 3_000));
  expect(wiped).toEqual([]);
  await next.close();
}, 60_000);

// E13: a sign-out here never wipes the replica another tab still has open
// (its user is still signed in there); the last tab to sign out wipes it.
it("leaves the replica and its entry while another tab holds it, and the last tab out wipes", async () => {
  stubWebLocks();
  const { open, storage, wiped } = await setup();
  const recorded = () =>
    [...storage].find(([key]) => key.startsWith("jazz-replica-v2:"))?.[1] ?? "";
  const here = await signInLocalFirst(open);
  cleanups.push(() => here.close());
  const there = await signInLocalFirst(open);
  cleanups.push(() => there.close());

  await here.logout();
  expect(here.getSnapshot().status).toBe("signed-out");
  await new Promise((resolve) => setTimeout(resolve, 500));
  expect(there.getSnapshot().status).toBe("ready");
  expect(wiped).toEqual([]);
  expect(recorded()).toContain('"subject":"alice"');
  // Once the other tab signs out too, nothing holds it: it wipes, and the entry goes.
  await there.logout();
  expect(wiped).toEqual(["alice"]);
  await expect.poll(recorded, { timeout: 5_000 }).toBe("");
}, 60_000);

// The other tab goes away without signing out, so its store is left
// behind; this page wipes it without a reload, on its next trigger (here it
// becomes visible), once the backend no longer names the user. Nothing polls.
it("wipes in the page once the tab that held the replica goes away, on the page's next trigger", async () => {
  stubWebLocks();
  const { jar, open, wiped, page } = await setup();
  const here = await signInLocalFirst(open);
  cleanups.push(() => here.close());
  const there = await signInLocalFirst(open);
  jar.user = null;
  await here.logout();
  expect(wiped).toEqual([]);
  // Closed (a crash, a reload elsewhere) before it heard of the sign-out.
  await there.close();
  await new Promise((resolve) => setTimeout(resolve, 3_000));
  expect(wiped).toEqual([]);
  page.dispatchEvent(new Event("visibilitychange"));
  await expect.poll(() => wiped, { timeout: 15_000 }).toEqual(["alice"]);
  await new Promise((resolve) => setTimeout(resolve, 1_000));
  expect(wiped).toEqual(["alice"]);
}, 90_000);

// A sign-out whose delete fails still signs out: the user asked to leave.
// The session says the wipe is unfinished, and a later reconcile finishes it.
it("signs out when the sign-out's delete fails, says so, and finishes the wipe later", async () => {
  stubWebLocks();
  const { jar, open, wiped, hold } = await setup();
  const here = await signInLocalFirst(open);
  cleanups.push(() => here.close());
  hold.fail = true;
  jar.user = null;
  vi.spyOn(console, "error").mockImplementation(() => {});
  await here.logout();
  expect(here.getSnapshot().status).toBe("signed-out");
  expect(here.getSnapshot().error?.message).toMatch(/couldn't finish wiping/);
  expect(wiped).toEqual([]);
  const next = await open();
  cleanups.push(() => next.close());
  await expect.poll(() => wiped, { timeout: 15_000 }).toEqual(["alice"]);
}, 60_000);

// #5397 review: with clearOnSignOut, a guest's key is forgotten only by the
// sign-out that wiped its store. A tab signing out while another still has
// the guest open keeps the key, and the last tab out decides.
it("keeps a guest's key while another tab has it open, and the last tab out forgets it and wipes", async () => {
  stubWebLocks();
  const { jar, open, wiped, store } = await setup({ guests: true });
  jar.user = null;
  const here = await open();
  cleanups.push(() => here.close());
  await here.createLocalFirst();
  await expect.poll(() => isThinDb(here.getSnapshot().client!.db), { timeout: 15_000 }).toBe(false);
  const guest = here.getSnapshot().account!.id;
  const there = await open();
  cleanups.push(() => there.close());
  await expect.poll(() => there.getSnapshot().account?.id, { timeout: 15_000 }).toBe(guest);
  await expect
    .poll(() => isThinDb(there.getSnapshot().client!.db), { timeout: 15_000 })
    .toBe(false);
  const roots = async () => JSON.parse((await store.read()) ?? "{}").roots as string[];
  const [root] = await roots();

  await here.logout();
  expect(wiped).toEqual([]);
  expect(await roots()).toEqual([root]);
  // The last tab out wipes, and only then is the key gone.
  await there.logout();
  expect(wiped).toHaveLength(1);
  expect(await roots()).toEqual([]);
}, 60_000);
