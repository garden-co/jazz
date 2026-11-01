import { randomUUID } from "node:crypto";
import { mkdtemp, rm, stat } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { createJazz, createJazzSession, memory, sqlite, type Jazz } from "./index.js";

const app = s.defineApp(
  {
    todos: s.table({ title: s.string(), owner: s.uuid() }, {}),
  },
  { migrations: "none" },
);
const permissions = s.definePermissions(app, ({ policy, session }) => {
  policy.todos.allowRead.where({ owner: session.user.account });
  policy.todos.allowInsert.where({ owner: session.user.account });
  policy.todos.allowUpdate.never();
  policy.todos.allowDelete.never();
});

const NodeWebSocket = globalThis.WebSocket;
const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
});

/** The test app's own auth: the user named in a header. */
function authenticate(request: Request) {
  const userId = request.headers.get("x-test-user");
  return userId ? { userId } : null;
}

const requestPaths: string[] = [];

async function serve(jazz: Jazz, options: { sockets: boolean }): Promise<string> {
  requestPaths.length = 0;
  const server: Server = createServer((req, res) => {
    requestPaths.push(req.url ?? "");
    jazz.nodeHandler(req, res, () => {
      res.statusCode = 404;
      res.end("app route");
    });
  });
  if (options.sockets) await jazz.attach(server);
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

/**
 * Act as a browser signed in to the app as `userId`: every request and
 * socket to the app's origin carries its session, as a cookie would.
 */
function signInAs(origin: string, userId: string) {
  const realFetch = globalThis.fetch;
  const RealWebSocket = globalThis.WebSocket;
  globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    const request = new Request(input, init);
    if (new URL(request.url).origin === origin) request.headers.set("x-test-user", userId);
    return realFetch(request);
  }) as typeof fetch;
  globalThis.WebSocket = class extends RealWebSocket {
    constructor(url: string | URL) {
      super(url, { headers: { "x-test-user": userId } } as never);
    }
  } as typeof WebSocket;
  cleanups.push(async () => {
    globalThis.fetch = realFetch;
    globalThis.WebSocket = RealWebSocket;
  });
}

async function userSession(appId: string, origin: string, userId: string) {
  signInAs(origin, userId);
  const session = await createJazzSession({
    appId,
    app,
    permissions,
    serverUrl: `${origin}/jazz`,
    driver: { type: "memory" },
  });
  cleanups.push(() => session.close());
  await session.loginWithBackend();
  const { client, status, error } = session.getSnapshot();
  if (status !== "ready") throw error ?? new Error(status);
  return client!;
}

describe("createJazz", () => {
  it("serves sync and sign-in from the app's backend with permissions from code", async () => {
    const appId = randomUUID();
    const jazz = await createJazz({ appId, app, permissions, store: memory(), authenticate });
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: true });

    // Signed out: the backend names no user.
    const signedOut = await fetch(`${origin}/jazz/apps/${appId}/accounts/backend-sign-in`, {
      method: "POST",
    });
    expect(signedOut.status).toBe(401);
    // Admin and backend routes never leave the process.
    for (const route of ["admin/deploy", "backend/admit", "admin/permissions"]) {
      const response = await fetch(`${origin}/jazz/apps/${appId}/${route}`, {
        method: "POST",
        headers: { "X-Jazz-Admin-Secret": "guess" },
      });
      expect(response.status).toBe(404);
    }
    // Requests outside the mount fall through to the app.
    expect(await (await fetch(`${origin}/other`)).text()).toBe("app route");

    const alice = await userSession(appId, origin, "alice");
    // The client holds no Jazz credential: its identity comes from the backend.
    expect(alice.session?.user?.identity).toEqual({
      issuer: `jazz:app:${appId}`,
      subject: "alice",
    });

    // A socket that claims to be alice while the backend says bob is refused.
    const forged = new NodeWebSocket(`${origin.replace("http", "ws")}/jazz/apps/${appId}/ws`, {
      headers: { "x-test-user": "bob" },
    } as never);
    forged.addEventListener("open", () =>
      forged.send(
        JSON.stringify({
          peer_identity: "forged",
          backend_session: { issuer: `jazz:app:${appId}`, user_id: "alice" },
        }),
      ),
    );
    const closed = await new Promise<number>((resolve) =>
      forged.addEventListener("close", (event) => resolve(event.code)),
    );
    expect(closed).toBe(1008);
    const row = await alice.db
      .insert(app.todos, { title: "from alice", owner: alice.session!.user!.account })
      .wait({ tier: "global" });

    // The backend's own db sees the row; a request scope sees only its user's rows.
    const seen = await jazz.db.all(app.todos.where({ id: row.id }));
    expect(seen.map((todo) => todo.title)).toEqual(["from alice"]);
    const asBob = await jazz.forRequest(
      new Request("http://app/x", { headers: { "x-test-user": "bob" } }),
    );
    expect(await asBob.all(app.todos)).toEqual([]);
    // The user synced over a socket, not the stream fallback.
    expect(requestPaths.some((path) => path.includes("/stream"))).toBe(false);
  }, 60_000);

  it("syncs over the HTTP stream when the host can't upgrade sockets", async () => {
    const appId = randomUUID();
    const jazz = await createJazz({ appId, app, permissions, store: memory(), authenticate });
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: false });

    // A page on this origin: the client treats /jazz as same-origin.
    const previous = (globalThis as { location?: unknown }).location;
    (globalThis as { location?: unknown }).location = {
      host: new URL(origin).host,
      href: `${origin}/`,
    };
    cleanups.push(async () => {
      (globalThis as { location?: unknown }).location = previous;
    });

    const alice = await userSession(appId, origin, "alice");
    const row = await alice.db
      .insert(app.todos, { title: "over the stream", owner: alice.session!.user!.account })
      .wait({ tier: "global" });
    const seen = await jazz.db.all(app.todos.where({ id: row.id }));
    expect(seen.map((todo) => todo.title)).toEqual(["over the stream"]);
    expect(requestPaths.some((path) => path.endsWith(`/apps/${appId}/stream`))).toBe(true);
  }, 60_000);

  it("keeps data and its signing key in the sqlite directory across restarts", async () => {
    const appId = randomUUID();
    const dir = await mkdtemp(join(tmpdir(), "jazz-embedded-"));
    cleanups.push(() => rm(dir, { recursive: true, force: true }));

    const first = await createJazz({ appId, app, permissions, store: sqlite({ path: dir }) });
    const inserted = await first.db
      .insert(app.todos, { title: "kept", owner: randomUUID() })
      .wait({ tier: "global" });
    await first.close();
    expect((await stat(join(dir, "auth-key.pem"))).mode & 0o777).toBe(0o600);

    const second = await createJazz({ appId, app, permissions, store: sqlite({ path: dir }) });
    cleanups.push(() => second.close());
    const rows = await second.db.all(app.todos.where({ id: inserted.id }));
    expect(rows.map((todo) => todo.title)).toEqual(["kept"]);
  }, 60_000);
});
