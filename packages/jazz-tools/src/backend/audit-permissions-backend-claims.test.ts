// Audit (state-space, permissions-backend): a signed-in user's claims change
// while their sync session is open. `recheckSeconds` re-asks `authenticate`,
// but only compares the user id, so the socket keeps the claims the backend
// vouched for when it opened (up to `sessionTtlSeconds`, an hour by default).
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { createJazz, createJazzClient, memory, type Jazz } from "./index.js";

const app = s.defineApp(
  { todos: s.table({ title: s.string(), owner: s.uuid() }, {}) },
  { migrations: "none" },
);
// Anyone reads their own todos; only an admin (a claim) may create one.
const permissions = s.definePermissions(app, ({ policy, session }) => {
  policy.todos.allowRead.where({ owner: session.user.account });
  policy.todos.allowInsert.where(session.where({ "claims.role": "admin" }));
  policy.todos.allowUpdate.never();
  policy.todos.allowDelete.never();
});

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
});

async function serve(jazz: Jazz, onUpgrade: () => void): Promise<string> {
  const server: Server = createServer((req, res) =>
    jazz.nodeHandler(req, res, () => {
      res.statusCode = 404;
      res.end();
    }),
  );
  server.on("upgrade", onUpgrade);
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

const outcome = (write: { wait(options: { tier: "global" }): Promise<unknown> }) =>
  write.wait({ tier: "global" }).then(
    () => "accepted",
    (error: { code?: string; message?: string }) => error.code ?? error.message,
  );

describe("audit: claims change mid-session", () => {
  it("a demoted user's open sync session stops carrying the old claims", async () => {
    const roles = new Map<string, string>([["alice", "admin"]]);
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: (request) => {
        const userId = request.headers.get("x-test-user");
        return userId ? { userId, claims: { role: roles.get(userId) ?? "none" } } : null;
      },
      recheckSeconds: 0.05,
    }).ready();
    cleanups.push(() => jazz.close());
    let upgrades = 0;
    const origin = await serve(jazz, () => upgrades++);
    signInAs(origin, "alice");
    const session = await createJazzClient({
      appId: jazz.appId,
      app,
      permissions,
      serverUrl: `${origin}/jazz`,
      driver: { type: "memory" },
    });
    cleanups.push(() => session.close());
    await session.loginWithBackend();
    const alice = session.getSnapshot().client!;
    const owner = alice.session!.user!.account;

    expect(await outcome(alice.db.insert(app.todos, { title: "as admin", owner }))).toBe(
      "accepted",
    );

    // The app demotes her. Her next request through forRequest sees it:
    const request = new Request(`${origin}/`, { headers: { "x-test-user": "alice" } });
    roles.set("alice", "viewer");
    const scoped = await jazz.forRequest(request);
    expect(await outcome(scoped.insert(app.todos, { title: "forRequest", owner }))).toBe(
      "permission_denied",
    );

    // Twenty rechecks later, her open socket must not still act as an admin.
    const before = upgrades;
    await new Promise((resolve) => setTimeout(resolve, 1_000));
    const write = await outcome(alice.db.insert(app.todos, { title: "after demotion", owner }));
    expect({ write, reconnected: upgrades > before }).toEqual({
      write: "permission_denied",
      reconnected: true,
    });
  }, 60_000);
});
