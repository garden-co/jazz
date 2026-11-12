import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { createJazz, memory } from "../backend/index.js";
import { createDb as createLocalFirstDb } from "../runtime/default-create-db.js";
import { isThinDb } from "../runtime/local-first-upgrade.js";
import type { AccountStore } from "../accounts/persistence.js";
import { createAccountManager } from "../accounts/create-account-manager.js";
import { createJazzClient } from "./create-jazz-session.js";

// A host without cookies (Node, a CLI) whose user the app's backend signs in
// with the app's own token: `bearer` on createJazzClient.

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

/** The app's own auth: `Authorization: Bearer token-<user>`, and nothing else. */
const seen: Array<string | null> = [];
function authenticate(request: Request) {
  const header = request.headers.get("authorization");
  seen.push(header);
  const match = /^Bearer token-(\w+)$/.exec(header ?? "");
  return match ? { userId: match[1]!, expiresAt: Math.floor(Date.now() / 1000) + 3600 } : null;
}

async function serve() {
  const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
  cleanups.push(() => jazz.close());
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
  return { jazz, origin: `http://127.0.0.1:${(server.address() as AddressInfo).port}` };
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

describe("a host without cookies signing in with the app's bearer", () => {
  it("syncs thin, then local-first, as the user the bearer names", async () => {
    const { jazz, origin } = await serve();
    const token = { value: "token-alice" as string | null };
    let reads = 0;
    let upgrade!: () => void;
    const upgraded = new Promise<void>((resolve) => (upgrade = resolve));
    const session = await createJazzClient({
      serverUrl: `${origin}/jazz`,
      store: memoryStore(),
      driver: { type: "memory" },
      bearer: () => (reads++, token.value),
      async client() {
        await upgraded;
        return { createClient: createLocalFirstDb };
      },
    });
    cleanups.push(() => session.close());
    // The backend named alice by the bearer: no sign-in call needed.
    expect(session.getSnapshot().status).toBe("ready");
    const thin = session.getSnapshot().client!;
    expect(isThinDb(thin.db)).toBe(true);
    expect(thin.session?.identity).toEqual({ issuer: "jazz:backend", subject: "alice" });
    const owner = thin.session!.user!;

    const row = await thin.db.insert(app.todos, { title: "thin", owner }).wait({ tier: "global" });
    const [written] = await jazz.db.all(app.todos.where({ id: row.id }).select("$createdBy"));
    expect(written?.$createdBy).toMatchObject({ account: owner });
    expect((await thin.db.all(app.todos)).map((todo) => todo.title)).toEqual(["thin"]);

    upgrade();
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
    // Read for each request and socket, so a refreshed token is used.
    expect(reads).toBeGreaterThanOrEqual(3);
    expect(seen).toContain("Bearer token-alice");

    // The app signs alice out: its bearer is gone, and so is her session.
    token.value = null;
    await session.reauthenticate();
    expect(session.getSnapshot().status).toBe("signed-out");
  }, 60_000);

  it("opens signed out while the bearer names no one, and signs in once it does", async () => {
    const { origin } = await serve();
    const token = { value: null as string | null };
    const session = await createJazzClient({
      serverUrl: `${origin}/jazz`,
      store: memoryStore(),
      driver: { type: "memory" },
      bearer: () => token.value,
    });
    cleanups.push(() => session.close());
    expect(session.getSnapshot().status).toBe("signed-out");
    token.value = "token-bob";
    await session.loginWithBackend();
    const { client } = session.getSnapshot();
    expect(client?.session?.identity.subject).toBe("bob");
    await client!.db
      .insert(app.todos, { title: "bob's", owner: client!.session!.user! })
      .wait({ tier: "global" });
  }, 30_000);

  it("sends the bearer only to the app's own endpoint", async () => {
    const { origin } = await serve();
    const elsewhere: Array<string | null> = [];
    const realFetch = globalThis.fetch;
    const session = await createJazzClient({
      serverUrl: `${origin}/jazz`,
      store: memoryStore(),
      driver: { type: "memory" },
      bearer: () => "token-alice",
      fetch: ((input: RequestInfo | URL, init?: RequestInit) => {
        const request = new Request(input, init);
        if (new URL(request.url).origin !== origin)
          elsewhere.push(request.headers.get("authorization"));
        return realFetch(request);
      }) as typeof fetch,
    });
    cleanups.push(() => session.close());
    expect(session.getSnapshot().status).toBe("ready");
    expect(elsewhere).toEqual([]);
  }, 30_000);

  it("is refused in a browser, whose sync connection can't carry it", async () => {
    vi.stubGlobal("document", {});
    await expect(
      createAccountManager({
        serverUrl: "https://app.example/jazz",
        store: memoryStore(),
        bearer: () => "token-alice",
      }),
    ).rejects.toThrow(/not supported yet in a browser/);
  });

  it("is refused on React Native, whose own entry sends the bearer", async () => {
    vi.stubGlobal("navigator", { product: "ReactNative" });
    await expect(
      createAccountManager({
        serverUrl: "https://app.example/jazz",
        store: memoryStore(),
        bearer: () => "token-alice",
      }),
    ).rejects.toThrow(
      /not supported on React Native: use createAccountManager from jazz-tools\/react-native/,
    );
  });
});
