import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtemp, readdir, rm, writeFile } from "node:fs/promises";
import { createServer, request as httpRequest, type IncomingMessage, type Server } from "node:http";
import { createRequire } from "node:module";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { generateSecret, identityOf, signChallenge, signToken } from "../local-first-auth/index.js";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { JazzCore } from "jazz-napi";
import { afterEach, describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { startLocalJazzServer } from "../dev/dev-server.js";
import {
  acceptLocalFirstAuth,
  createJazz,
  createJazzClient,
  files,
  fromJWT,
  isRequestAuthenticationError,
  memory,
  remote,
  sqlite,
  vercel,
  type Jazz,
  type RequestAuthenticationError,
} from "./index.js";
import { localFirstAccountId } from "../accounts/local-first.js";
import {
  encodeWebSocketFrameBatch,
  encodeWireClientHello,
} from "../runtime/native-runtime/websocket.js";

const app = s.defineApp(
  {
    todos: s.table({ title: s.string(), owner: s.uuid() }, {}),
  },
  { migrations: "none" },
);
const permissions = s.definePermissions(app, ({ policy, session }) => {
  policy.todos.allowRead.where({ owner: session.user });
  policy.todos.allowInsert.where({ owner: session.user });
  policy.todos.allowUpdate.never();
  policy.todos.allowDelete.never();
});

const NodeWebSocket = globalThis.WebSocket;
const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
});

/** The test app's own auth: the user named in a header, else a local-first guest. */
function authenticate(request: Request) {
  const userId = request.headers.get("x-test-user");
  return userId ? { userId } : acceptLocalFirstAuth(request);
}

const requestPaths: string[] = [];
/** The stream key each request carried, by request path. */
const streamKeys = new Map<string, string>();

async function serve(
  jazz: Jazz,
  options: { sockets: boolean; onUpgrade?: () => void },
): Promise<string> {
  requestPaths.length = 0;
  streamKeys.clear();
  const server: Server = createServer((req, res) => {
    requestPaths.push(req.url ?? "");
    const key = req.headers["x-jazz-stream-key"];
    if (typeof key === "string") streamKeys.set(req.url ?? "", key);
    jazz.nodeHandler(req, res, () => {
      res.statusCode = 404;
      res.end("app route");
    });
  });
  if (options.onUpgrade) server.on("upgrade", options.onUpgrade);
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

/** A request's status, sent past `signInAs` (as another browser would). */
function statusOf(url: string, method: string, headers: Record<string, string>): Promise<number> {
  return new Promise((resolve, reject) => {
    const req = httpRequest(url, { method, headers }, (res) => {
      res.resume();
      resolve(res.statusCode ?? 0);
    });
    req.on("error", reject);
    req.end();
  });
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
    constructor(url: string | URL, options?: { headers?: Record<string, string> }) {
      // Sockets to the app's origin carry the session; a backend's own
      // sockets to its core keep their headers.
      const headers = new URL(url).host === new URL(origin).host ? { "x-test-user": userId } : {};
      super(url, { ...options, headers: { ...options?.headers, ...headers } } as never);
    }
  } as typeof WebSocket;
  cleanups.push(async () => {
    globalThis.fetch = realFetch;
    globalThis.WebSocket = RealWebSocket;
  });
}

async function userSession(appId: string, origin: string, userId: string) {
  signInAs(origin, userId);
  const session = await createJazzClient({
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
    const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    const appId = jazz.appId;
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: true });

    // Signed out: the backend names no user.
    const signedOut = await fetch(`${origin}/jazz/apps/${appId}/accounts/backend-sign-in`, {
      method: "POST",
    });
    expect(signedOut.status).toBe(401);
    // Admin and backend routes never leave the process.
    for (const route of ["admin/bundles", "backend/admit"]) {
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
    expect(alice.session?.identity).toEqual({
      issuer: "jazz:backend",
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
          backend_session: { issuer: "jazz:backend", user_id: "alice" },
        }),
      ),
    );
    const closed = await new Promise<number>((resolve) =>
      forged.addEventListener("close", (event) => resolve(event.code)),
    );
    expect(closed).toBe(1008);
    // Browser worker transports send `jwt_token: null` next to the session the
    // backend vouches for: that is no token, and the socket is admitted.
    const fromWorker = new NodeWebSocket(`${origin.replace("http", "ws")}/jazz/apps/${appId}/ws`, {
      headers: { "x-test-user": "alice" },
    } as never);
    fromWorker.addEventListener("open", () => {
      fromWorker.send(
        JSON.stringify({
          peer_identity: "worker",
          jwt_token: null,
          backend_session: { issuer: "jazz:backend", user_id: "alice" },
          auth: { jwt_token: null, backend_session: { user_id: "alice" } },
        }),
      );
      fromWorker.send(encodeWebSocketFrameBatch([encodeWireClientHello()]));
    });
    const answered = await new Promise<string>((resolve) => {
      fromWorker.addEventListener("message", () => resolve("message"));
      fromWorker.addEventListener("close", (event) => resolve(`closed ${event.code}`));
    });
    fromWorker.close();
    expect(answered).toBe("message");
    const row = await alice.db
      .insert(app.todos, { title: "from alice", owner: alice.session!.user! })
      .wait({ tier: "global" });

    // The backend's own db sees the row; a request scope sees only its user's rows.
    const seen = await jazz.db.all(app.todos.where({ id: row.id }));
    expect(seen.map((todo) => todo.title)).toEqual(["from alice"]);
    const asBob = await jazz.forReqUser(
      new Request("http://app/x", { headers: { "x-test-user": "bob" } }),
    );
    expect(await asBob.all(app.todos)).toEqual([]);
    // API routes can name the request's account without a client credential.
    await expect(
      jazz.accountFor(new Request("http://app/x", { headers: { "x-test-user": "alice" } })),
    ).resolves.toBe(alice.session!.user!);
    await expect(jazz.accountFor(new Request("http://app/x"))).resolves.toBeNull();
    // Server actions: backend authority, writes stamped as the request's user.
    const onBehalf = await jazz.withAttribution(
      new Request("http://app/x", { headers: { "x-test-user": "alice" } }),
    );
    const stamped = await onBehalf
      .insert(app.todos, { title: "on alice's behalf", owner: randomUUID() })
      .wait({ tier: "global" });
    const [withProvenance] = await jazz.db.all(
      app.todos.where({ id: stamped.id }).select("title", "$createdBy"),
    );
    expect(withProvenance?.$createdBy).toMatchObject({ account: alice.session!.user! });
    // The user synced over a socket, not the stream fallback.
    expect(requestPaths.some((path) => path.includes("/stream"))).toBe(false);
    // This host upgrades sockets, so a failed socket isn't sent to the stream:
    // the probe gets no stream marker and the client retries the socket.
    const probe = await fetch(`${origin}/jazz/apps/${appId}/ws`);
    expect(probe.status).toBe(426);
    expect(probe.headers.get("x-jazz-stream")).toBeNull();
  }, 60_000);

  for (const sockets of [true, false]) {
    it(`lets go of its clients' ${sockets ? "sockets" : "HTTP streams"} when it closes, so its server can`, async () => {
      const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
      let closed = false;
      cleanups.push(() => (closed ? Promise.resolve() : jazz.close()));
      // The host's own server, closed the way a SIGTERM handler closes it:
      // without dropping the connections it still serves.
      const server: Server = createServer((req, res) => jazz.nodeHandler(req, res));
      if (sockets) await jazz.attach(server);
      await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
      cleanups.push(
        () =>
          new Promise((resolve) => {
            server.closeAllConnections();
            server.close(resolve);
          }),
      );
      const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
      if (!sockets) {
        // A page on this origin, so the client falls back to the stream.
        const previous = (globalThis as { location?: unknown }).location;
        (globalThis as { location?: unknown }).location = {
          host: new URL(origin).host,
          href: `${origin}/`,
        };
        cleanups.push(async () => {
          (globalThis as { location?: unknown }).location = previous;
        });
      }
      const alice = await userSession(jazz.appId, origin, "alice");
      await alice.db
        .insert(app.todos, { title: "synced", owner: alice.session!.user!.account })
        .wait({ tier: "global" });

      await jazz.close();
      closed = true;
      const serverClosed = new Promise<void>((resolve) => server.close(() => resolve()));
      await Promise.race([
        serverClosed,
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error("a client connection outlived close()")), 5_000),
        ),
      ]);
    }, 30_000);
  }

  for (const host of ["close-server", "no-handler"] as const) {
    it(`stops on SIGTERM with a client stream open (${host})`, async () => {
      // tsx is a workspace dev dependency: resolve its CLI the way Node does
      // instead of trusting a package's own node_modules/.bin.
      const tsx = createRequire(import.meta.url).resolve("tsx/cli");
      const child = spawn(
        process.execPath,
        [tsx, fileURLToPath(new URL("./fixtures/signal-host.ts", import.meta.url)), host],
        { stdio: ["ignore", "pipe", "inherit"] },
      );
      const exited = new Promise<{ code: number | null; signal: string | null }>((resolve) =>
        child.on("exit", (code, signal) => resolve({ code, signal })),
      );
      cleanups.push(async () => void child.kill("SIGKILL"));
      const ready = await new Promise<string>((resolve, reject) => {
        child.stdout!.on("data", (chunk: Buffer) => resolve(`${chunk}`.trim()));
        child.on("error", reject);
        child.on("exit", () => reject(new Error("the host exited before it served")));
      });
      const [port, appId] = ready.split(" ");
      // A browser's sync stream, open until the server ends it.
      const stream = await fetch(`http://127.0.0.1:${port}/jazz/apps/${appId}/stream`, {
        method: "POST",
      });
      expect(stream.status).toBe(200);
      const reader = stream.body!.getReader();
      await reader.read();

      child.kill("SIGTERM");
      const outcome = await Promise.race([
        exited,
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error("the host outlived SIGTERM")), 15_000),
        ),
      ]);
      // Stopped by the signal itself, as with no handler at all (tsx, which
      // runs the host, reports its child's signal as 128 + 15).
      if (host === "close-server") expect(outcome).toEqual({ code: 0, signal: null });
      else expect(outcome.signal === "SIGTERM" || outcome.code === 128 + 15).toBe(true);
      void reader.cancel().catch(() => undefined);
    }, 60_000);
  }

  it("answers a signed-out request with an authentication error, not a plain one", async () => {
    const failure = new Error("auth service down");
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: (request) => {
        if (request.headers.has("x-fail")) throw failure;
        return request.headers.get("x-test-user") ? { userId: "alice" } : null;
      },
    }).ready();
    cleanups.push(() => jazz.close());
    const signedOut = new Request("http://app/x");
    for (const scope of [jazz.forReqUser, jazz.withAttribution]) {
      const error = await scope(signedOut).catch((caught: unknown) => caught);
      // What an app maps to 401.
      expect(isRequestAuthenticationError(error)).toBe(true);
      expect((error as RequestAuthenticationError).code).toBe("signed_out");
      expect((error as Error).message).toMatch(/has no signed-in user/);
    }
    // authenticate failing is the server's problem, and stays its own error.
    await expect(
      jazz.forReqUser(new Request("http://app/x", { headers: { "x-fail": "1" } })),
    ).rejects.toBe(failure);
  });

  it("leaves a body a parser read before nodeHandler to the app's routes", async () => {
    const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    cleanups.push(() => jazz.close());
    // What express.json() does before the app's handlers: read the stream
    // whole and leave the result on `req.body`.
    const parse = (req: IncomingMessage & { body?: unknown }, done: () => void) => {
      const chunks: Buffer[] = [];
      req.on("data", (chunk: Buffer) => chunks.push(chunk));
      req.on("end", () => {
        const raw = Buffer.concat(chunks);
        req.body = req.headers["content-type"] === "application/json" ? JSON.parse(`${raw}`) : raw;
        done();
      });
    };
    const server: Server = createServer((req, res) =>
      parse(req, () =>
        jazz.nodeHandler(req, res, () => {
          res.setHeader("content-type", "application/json");
          res.end(JSON.stringify({ got: (req as { body?: unknown }).body }));
        }),
      ),
    );
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    cleanups.push(() => new Promise((resolve) => server.close(resolve)));
    const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;

    const echoed = await fetch(`${origin}/api/echo`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: '{"a":1}',
    });
    expect(echoed.status).toBe(200);
    expect(await echoed.json()).toEqual({ got: { a: 1 } });

    // A Jazz route whose body a JSON parser took fails plainly, naming the
    // fix, instead of with a locked-stream error.
    const errors = vi.spyOn(console, "error").mockImplementation(() => undefined);
    try {
      const taken = await fetch(`${origin}/jazz/auth/sign-in`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "{}",
      });
      expect(taken.status).toBe(500);
      expect(errors.mock.calls.flat().join(" ")).toMatch(
        /mount jazz.nodeHandler before body parsers/,
      );
    } finally {
      errors.mockRestore();
    }
  });

  it("signs users in with fromJWT and keeps their account and claims", async () => {
    const { privateKey, publicKey } = await generateKeyPair("ES256", { extractable: true });
    const issuer = "https://idp.example";
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: fromJWT({ publicKey: await exportJWK(publicKey), issuer, audience: "my-app" }),
    }).ready();
    cleanups.push(() => jazz.close());
    const bearer = async (subject: string) =>
      new Request("http://app/x", {
        headers: {
          authorization: `Bearer ${await new SignJWT({ role: "editor" })
            .setProtectedHeader({ alg: "ES256" })
            .setIssuer(issuer)
            .setAudience("my-app")
            .setSubject(subject)
            .setExpirationTime("5m")
            .sign(privateKey)}`,
        },
      });
    const account = await jazz.accountFor(await bearer("user_01"));
    expect(account).toEqual(expect.any(String));
    // A fresh token for the same user opens the same account.
    await expect(jazz.accountFor(await bearer("user_01"))).resolves.toBe(account);
    await expect(jazz.accountFor(await bearer("user_02"))).resolves.not.toBe(account);
    await expect(jazz.accountFor(new Request("http://app/x"))).resolves.toBeNull();
  }, 60_000);

  it("links only a guest whose proof is for this app or this endpoint", async () => {
    const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    const appId = jazz.appId;
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: true });
    const secret = generateSecret();
    const link = async (audience: string, headers: Record<string, string> = {}) => {
      const accounts = `${origin}/jazz/apps/${appId}/accounts`;
      const user = { "x-test-user": `user-for-${audience}`, "content-type": "application/json" };
      const { challenge } = (await (
        await fetch(`${accounts}/backend-link/challenge`, { method: "POST", headers: user })
      ).json()) as { challenge: string };
      const response = await fetch(`${accounts}/backend-link`, {
        method: "POST",
        headers: { ...user, ...headers },
        body: JSON.stringify({ challenge, proof: signChallenge(secret, challenge), audience }),
      });
      return response.status;
    };

    // Another app's backend replaying a guest's proof here gets nothing.
    expect(await link("https://elsewhere.example/jazz")).toBe(401);
    // Nor does naming that other endpoint in headers a client can forge.
    const forged = { "x-forwarded-host": "elsewhere.example", "x-forwarded-proto": "https" };
    expect(await link("https://elsewhere.example/jazz", forged)).toBe(401);
    expect(
      await link("https://elsewhere.example/jazz", {
        ...forged,
        origin: "https://elsewhere.example",
      }),
    ).toBe(401);
    // This host over https is not the plain-http endpoint the test serves.
    expect(await link(`${origin.replace("http:", "https:")}/jazz`)).toBe(401);
    expect(await link(randomUUID())).toBe(401);
    expect(await link(`${origin}/jazz`)).toBe(200);
    // The same key, already founded under the endpoint, is not founded again.
    expect(await link(appId)).toBe(409);
  });

  it("accepts guests of the public URL a proxy serves this endpoint at", async () => {
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate,
      publicUrl: "https://app.example.com/api/jazz/",
    }).ready();
    const appId = jazz.appId;
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: true });
    const secret = generateSecret();
    const link = async (audience: string) => {
      const accounts = `${origin}/jazz/apps/${appId}/accounts`;
      const user = { "x-test-user": `user-for-${audience}`, "content-type": "application/json" };
      const { challenge } = (await (
        await fetch(`${accounts}/backend-link/challenge`, { method: "POST", headers: user })
      ).json()) as { challenge: string };
      return (
        await fetch(`${accounts}/backend-link`, {
          method: "POST",
          headers: user,
          body: JSON.stringify({ challenge, proof: signChallenge(secret, challenge), audience }),
        })
      ).status;
    };
    expect(await link(`${origin}/jazz`)).toBe(401);
    expect(await link("https://app.example.com/api/jazz")).toBe(200);
    // Checked when createJazz is called, before anything opens.
    expect(() =>
      createJazz({ app, permissions, store: memory(), authenticate, publicUrl: "/jazz" }),
    ).toThrow("not an absolute URL");
  });

  it("refuses sign-in and sync started by a page on another origin", async () => {
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate,
      // Written as a URL, compared as an origin.
      allowedOrigins: ["https://app.example.com/"],
    }).ready();
    const appId = jazz.appId;
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: true });
    const signIn = (headers: Record<string, string>) =>
      fetch(`${origin}/jazz/apps/${appId}/accounts/backend-sign-in`, {
        method: "POST",
        headers: { "x-test-user": "alice", ...headers },
      }).then((response) => response.status);

    // The user's cookie rides along, but the page is elsewhere.
    expect(await signIn({ origin: "https://evil.example" })).toBe(403);
    expect(await signIn({ origin: "null" })).toBe(403);
    expect(await signIn({ "sec-fetch-site": "cross-site" })).toBe(403);
    // The right host, but a plain-http page acting on a backend reached over https.
    expect(await signIn({ origin, "x-forwarded-proto": "https" })).toBe(403);
    for (const route of ["accounts/backend-link", "accounts/backend-link/challenge", "stream"]) {
      const response = await fetch(`${origin}/jazz/apps/${appId}/${route}`, {
        method: "POST",
        headers: { "x-test-user": "alice", origin: "https://evil.example" },
      });
      expect(response.status).toBe(403);
    }
    // The backend's own origin, an allowed one, and a non-browser client pass.
    expect(await signIn({ origin })).toBe(200);
    expect(await signIn({ origin: origin.replace("http:", "https:") })).toBe(200);
    expect(await signIn({ origin: "https://app.example.com" })).toBe(200);
    expect(await signIn({})).toBe(200);

    const socketFrom = (from: string) =>
      new Promise<string>((resolve) => {
        const socket = new NodeWebSocket(`${origin.replace("http", "ws")}/jazz/apps/${appId}/ws`, {
          headers: { "x-test-user": "alice", origin: from },
        } as never);
        socket.addEventListener("open", () => {
          socket.send(
            JSON.stringify({
              peer_identity: "page",
              backend_session: { issuer: "jazz:backend", user_id: "alice" },
            }),
          );
          socket.send(encodeWebSocketFrameBatch([encodeWireClientHello()]));
        });
        socket.addEventListener("message", () => {
          socket.close();
          resolve("admitted");
        });
        socket.addEventListener("error", () => resolve("refused"));
        socket.addEventListener("close", () => resolve("refused"));
      });
    expect(await socketFrom("https://evil.example")).toBe("refused");
    expect(await socketFrom(origin)).toBe("admitted");
  }, 60_000);

  it("ends a user's open sync session once authenticate no longer names them", async () => {
    const signedOut = new Set<string>();
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: (request) => {
        const userId = request.headers.get("x-test-user");
        return userId && !signedOut.has(userId) ? { userId } : null;
      },
      recheckSeconds: 0.05,
    }).ready();
    const appId = jazz.appId;
    cleanups.push(() => jazz.close());
    let upgrades = 0;
    const origin = await serve(jazz, { sockets: true, onUpgrade: () => upgrades++ });
    const alice = await userSession(appId, origin, "alice");
    await alice.db
      .insert(app.todos, { title: "signed in", owner: alice.session!.user! })
      .wait({ tier: "global" });

    // Checks that still name her leave her session open.
    const opened = upgrades;
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(upgrades).toBe(opened);

    // She signs out elsewhere: the session ends, and the client's reconnects
    // are refused.
    signedOut.add("alice");
    await expect.poll(() => upgrades, { timeout: 5_000, interval: 50 }).toBeGreaterThan(opened);
    // A refused reconnect is an auth failure, so the client stops retrying
    // and its session can ask the backend whether anyone is signed in.
    await expect
      .poll(() => alice.db.getSyncStatus().link, { timeout: 10_000, interval: 50 })
      .toMatchObject({ state: "offline", reason: "auth" });
    const refused = upgrades;
    await new Promise((resolve) => setTimeout(resolve, 1_000));
    expect(upgrades).toBe(refused);
  }, 30_000);

  it("ends a user's open sync session once their claims change, so policies see the new ones", async () => {
    // Anyone reads their own todos; only an admin (a claim) may create one.
    const byRole = s.definePermissions(app, ({ policy, session }) => {
      policy.todos.allowRead.where({ owner: session.user });
      policy.todos.allowInsert.where(session.where({ "claims.role": "admin" }));
      policy.todos.allowUpdate.never();
      policy.todos.allowDelete.never();
    });
    const roles = new Map([["alice", "admin"]]);
    const jazz = await createJazz({
      app,
      permissions: byRole,
      store: memory(),
      authenticate: (request) => {
        const userId = request.headers.get("x-test-user");
        return userId ? { userId, claims: { role: roles.get(userId) ?? "none" } } : null;
      },
      recheckSeconds: 0.05,
    }).ready();
    cleanups.push(() => jazz.close());
    let upgrades = 0;
    const origin = await serve(jazz, { sockets: true, onUpgrade: () => upgrades++ });
    signInAs(origin, "alice");
    const session = await createJazzClient({
      appId: jazz.appId,
      app,
      permissions: byRole,
      serverUrl: `${origin}/jazz`,
      driver: { type: "memory" },
    });
    cleanups.push(() => session.close());
    await session.loginWithBackend();
    const alice = session.getSnapshot().client!;
    const owner = alice.session!.user!;
    const outcome = (write: { wait(options: { tier: "global" }): Promise<unknown> }) =>
      write.wait({ tier: "global" }).then(
        () => "accepted",
        (error: { code?: string; message?: string }) => error.code ?? error.message,
      );
    expect(await outcome(alice.db.insert(app.todos, { title: "as admin", owner }))).toBe(
      "accepted",
    );

    // Checks that name her with the same claims leave her session open.
    const opened = upgrades;
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(upgrades).toBe(opened);

    // The app demotes her: her open session ends, and the client reconnects
    // with the claims authenticate names now, as forReqUser already sees.
    roles.set("alice", "viewer");
    const scoped = await jazz.forReqUser(
      new Request(`${origin}/`, { headers: { "x-test-user": "alice" } }),
    );
    expect(await outcome(scoped.insert(app.todos, { title: "forReqUser", owner }))).toBe(
      "permission_denied",
    );
    await expect.poll(() => upgrades, { timeout: 5_000, interval: 50 }).toBeGreaterThan(opened);
    await expect
      .poll(() => alice.db.getSyncStatus().link, { timeout: 10_000, interval: 50 })
      .toEqual({ state: "connected" });
    expect(await outcome(alice.db.insert(app.todos, { title: "after demotion", owner }))).toBe(
      "permission_denied",
    );
    // Still signed in: one reconnect, not a loop.
    const reconnected = upgrades;
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(upgrades).toBe(reconnected);
  }, 60_000);

  it("refuses local-first guests unless authenticate accepts them", async () => {
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: (request) => {
        const userId = request.headers.get("x-test-user");
        return userId ? { userId } : null;
      },
    });
    cleanups.push(() => jazz.close());
    let upgrades = 0;
    const origin = await serve(jazz, { sockets: true, onUpgrade: () => upgrades++ });
    const guestSession = await createJazzClient({
      app,
      permissions,
      serverUrl: `${origin}/jazz`,
      driver: { type: "memory" },
      initial: "local-first",
    });
    cleanups.push(() => guestSession.close());
    const guest = guestSession.getSnapshot().client!;
    await guest.db.insert(app.todos, {
      title: "kept on the device",
      owner: guest.session!.user!,
    });

    // The socket is refused as an auth failure, so the client stops retrying.
    await expect
      .poll(() => guest.db.getSyncStatus().link, { timeout: 10_000, interval: 50 })
      .toMatchObject({ state: "offline", reason: "auth" });
    expect(upgrades).toBeGreaterThan(0);
    expect(await jazz.db.all(app.todos)).toEqual([]);
  }, 30_000);

  it("ends a guest's open sync session once authenticate stops accepting guests", async () => {
    let guestsWelcome = true;
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: (request) => (guestsWelcome ? authenticate(request) : null),
      recheckSeconds: 0.05,
    });
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: true });
    const guestSession = await createJazzClient({
      app,
      permissions,
      serverUrl: `${origin}/jazz`,
      driver: { type: "memory" },
      initial: "local-first",
    });
    cleanups.push(() => guestSession.close());
    const guest = guestSession.getSnapshot().client!;
    await guest.db
      .insert(app.todos, { title: "synced", owner: guest.session!.user! })
      .wait({ tier: "global" });

    guestsWelcome = false;
    await expect
      .poll(() => guest.db.getSyncStatus().link, { timeout: 10_000, interval: 50 })
      .toMatchObject({ state: "offline", reason: "auth" });
  }, 30_000);

  it("reconnects a user whose socket's cookies expired but who is still signed in", async () => {
    // Requests seen before `expiredBefore` carry cookies that have since expired.
    const firstSeen = new WeakMap<Request, number>();
    let expiredBefore = 0;
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: (request) => {
        if (!firstSeen.has(request)) firstSeen.set(request, Date.now());
        return firstSeen.get(request)! < expiredBefore ? null : authenticate(request);
      },
      recheckSeconds: 0.05,
    }).ready();
    const appId = jazz.appId;
    cleanups.push(() => jazz.close());
    let upgrades = 0;
    const origin = await serve(jazz, { sockets: true, onUpgrade: () => upgrades++ });
    const alice = await userSession(appId, origin, "alice");
    await alice.db
      .insert(app.todos, { title: "before", owner: alice.session!.user! })
      .wait({ tier: "global" });
    const opened = upgrades;
    expiredBefore = Date.now() + 1;
    // The re-check on the old socket fails; the client reconnects with fresh
    // cookies and keeps syncing.
    await expect.poll(() => upgrades, { timeout: 5_000, interval: 50 }).toBeGreaterThan(opened);
    await alice.db
      .insert(app.todos, { title: "after", owner: alice.session!.user! })
      .wait({ tier: "global" });
    expect(alice.db.getSyncStatus().link).toEqual({ state: "connected" });
  }, 30_000);

  it("links a second identity to the same account", async () => {
    const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    const appId = jazz.appId;
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: true });

    const guest = await userSession(appId, origin, "guest-device-key");
    const account = guest.session!.user!;
    const row = await guest.db
      .insert(app.todos, { title: "made as a guest", owner: account })
      .wait({ tier: "global" });

    // The app's sign-up verified both identities, then links them.
    await expect(jazz.linkIdentity({ from: "guest-device-key", to: "alice" })).resolves.toEqual({
      account,
    });

    const alice = await userSession(appId, origin, "alice");
    expect(alice.session!.user!).toBe(account);
    const seen = await alice.db.all(app.todos.where({ id: row.id }));
    expect(seen.map((todo) => todo.title)).toEqual(["made as a guest"]);
  }, 60_000);

  it.each([
    ["memory", "sockets", "names the app"],
    ["sqlite", "the HTTP stream", "names the app"],
    ["memory", "sockets", "is scoped by its endpoint"],
    ["sqlite", "the HTTP stream", "is scoped by its endpoint"],
  ] as const)(
    "keeps a local-first guest's account when it signs up (%s store, %s, the client %s)",
    async (kind, transport, scoping) => {
      const dir = await mkdtemp(join(tmpdir(), "jazz-link-"));
      cleanups.push(() => rm(dir, { recursive: true, force: true }));
      const store = kind === "memory" ? memory() : sqlite({ path: dir });
      const jazz = await createJazz({ app, permissions, store, authenticate }).ready();
      const appId = jazz.appId;
      cleanups.push(() => jazz.close());
      const origin = await serve(jazz, { sockets: transport === "sockets" });
      if (transport !== "sockets") {
        // A page on this origin: the client treats /jazz as same-origin.
        const previous = (globalThis as { location?: unknown }).location;
        (globalThis as { location?: unknown }).location = {
          host: new URL(origin).host,
          href: `${origin}/`,
        };
        cleanups.push(async () => {
          (globalThis as { location?: unknown }).location = previous;
        });
      }

      // A browser client names no app: the backend vouches for the endpoint
      // its guest's proof is for.
      const session = await createJazzClient({
        ...(scoping === "names the app" ? { appId } : {}),
        app,
        permissions,
        serverUrl: `${origin}/jazz`,
        driver: { type: "memory" },
        initial: "local-first",
      });
      cleanups.push(() => session.close());
      const guest = session.getSnapshot().client!;
      const account = guest.session!.user!;
      expect(account).toBe(
        localFirstAccountId(
          scoping === "names the app" ? appId : `${origin}/jazz`,
          guest.session!.identity.subject,
        ),
      );
      const row = await guest.db
        .insert(app.todos, { title: "made as a guest", owner: account })
        .wait({ tier: "global" });

      // The app's sign-up created "alice"; the backend now names her.
      signInAs(origin, "alice");
      await session.linkWithBackend();
      const alice = session.getSnapshot().client!;
      expect(alice.session!.user!).toBe(account);
      expect(alice.session!.identity).toEqual({
        issuer: "jazz:backend",
        subject: "alice",
      });
      const seen = await alice.db.all(app.todos.where({ id: row.id }));
      expect(seen.map((todo) => todo.title)).toEqual(["made as a guest"]);
      // The linked account syncs as the signed-in user.
      const after = await alice.db
        .insert(app.todos, { title: "made after signing up", owner: account })
        .wait({ tier: "global" });
      const stored = await jazz.db.all(app.todos.where({ id: after.id }));
      expect(stored.map((todo) => todo.title)).toEqual(["made after signing up"]);
    },
    60_000,
  );

  it("links a guest only on a fresh proof for one challenge, never on its token", async () => {
    const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    const appId = jazz.appId;
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: true });
    const accounts = `${origin}/jazz/apps/${appId}/accounts`;
    const post = (path: string, user: string, init: RequestInit = {}) =>
      fetch(`${accounts}/${path}`, {
        ...init,
        method: "POST",
        headers: { "x-test-user": user, "content-type": "application/json", ...init.headers },
      });
    const challengeFor = async (user: string) =>
      ((await (await post("backend-link/challenge", user)).json()) as { challenge: string })
        .challenge;
    const link = (user: string, body: unknown) =>
      post("backend-link", user, { body: JSON.stringify(body) });

    const secret = generateSecret();
    const guest = identityOf(secret).subject;
    // The guest's own (unexpired, or leaked) sync token links nothing.
    const token = signToken(secret, { audience: appId });
    const withToken = await post("backend-link", "mallory", {
      headers: { authorization: `Bearer ${token}` },
    });
    expect(withToken.status).toBe(400);
    // Signed out: no challenge.
    expect((await fetch(`${accounts}/backend-link/challenge`, { method: "POST" })).status).toBe(
      401,
    );

    const challenge = await challengeFor("alice");
    const proof = signChallenge(secret, challenge);
    // A proof for alice's challenge links no one else, and a forged or
    // tampered challenge links nothing.
    expect((await link("mallory", { challenge, proof })).status).toBe(400);
    const [payload, mac] = challenge.split(".");
    const forged = `${Buffer.from(
      JSON.stringify({
        ...JSON.parse(Buffer.from(payload!, "base64url").toString()),
        sub: "mallory",
      }),
    ).toString("base64url")}.${mac}`;
    expect(
      (await link("mallory", { challenge: forged, proof: signChallenge(secret, forged) })).status,
    ).toBe(400);
    // Another key's proof for the challenge doesn't prove this guest.
    expect(
      (await link("alice", { challenge, proof: signChallenge(generateSecret(), `${challenge}x`) }))
        .status,
    ).toBe(401);

    const linked = await link("alice", { challenge, proof });
    expect(linked.status).toBe(200);
    const { account } = (await linked.json()) as { account: string };
    expect(account).toBe(
      ((await (await post("backend-sign-in", "alice")).json()) as { account: string }).account,
    );
    // The same proof again is refused, even for the user it named: its
    // nonce is used. A fresh challenge for alice is a repeat of a link that
    // stands, and answers it.
    expect((await link("alice", { challenge, proof })).status).toBe(409);
    const fresh = await challengeFor("alice");
    const repeated = await link("alice", { challenge: fresh, proof: signChallenge(secret, fresh) });
    expect(repeated.status).toBe(200);
    expect(((await repeated.json()) as { account: string }).account).toBe(account);

    // An expired challenge links nothing.
    const stale = await challengeFor("carol");
    const realNow = Date.now;
    Date.now = () => realNow() + 121_000;
    try {
      expect(
        (await link("carol", { challenge: stale, proof: signChallenge(secret, stale) })).status,
      ).toBe(400);
    } finally {
      Date.now = realNow;
    }
    // Founding is the guest's own account, whatever was tried before.
    expect(account).toBe(
      ((await (await post("backend-sign-in", "alice")).json()) as { account: string }).account,
    );
    expect(guest).toMatch(/^[0-9a-f-]{36}$/);
  }, 60_000);

  it.each(["keep", "revoke"] as const)(
    "keeps or retires the guest key after a link (guestKeyAfterLink: %s)",
    async (guestKeyAfterLink) => {
      const jazz = await createJazz({
        app,
        permissions,
        store: memory(),
        authenticate,
        guestKeyAfterLink,
      }).ready();
      const appId = jazz.appId;
      cleanups.push(() => jazz.close());
      const origin = await serve(jazz, { sockets: true });
      const accounts = `${origin}/jazz/apps/${appId}/accounts`;
      const post = (path: string, user: string, body?: unknown) =>
        fetch(`${accounts}/${path}`, {
          method: "POST",
          headers: { "x-test-user": user, "content-type": "application/json" },
          body: body === undefined ? undefined : JSON.stringify(body),
        });
      const linkAs = async (user: string, secret: string) => {
        const { challenge } = (await (await post("backend-link/challenge", user)).json()) as {
          challenge: string;
        };
        return post("backend-link", user, { challenge, proof: signChallenge(secret, challenge) });
      };

      const secret = generateSecret();
      const linked = await linkAs("alice", secret);
      expect(linked.status).toBe(200);
      const { account } = (await linked.json()) as { account: string };
      // The guest key, used again to bring another user into the account:
      // a kept key still can, a revoked one opens nothing.
      const again = await linkAs("bob", secret);
      if (guestKeyAfterLink === "keep") {
        expect(again.status).toBe(200);
        expect(((await again.json()) as { account: string }).account).toBe(account);
      } else {
        expect(again.status).toBe(409);
      }
      // The linked user signs in either way.
      const alice = (await (await post("backend-sign-in", "alice")).json()) as { account: string };
      expect(alice.account).toBe(account);
    },
    60_000,
  );

  it("says who is signed in without giving them an account, so a guest can still link", async () => {
    const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    const appId = jazz.appId;
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: true });
    const accounts = `${origin}/jazz/apps/${appId}/accounts`;
    const post = (path: string, user?: string, body?: unknown) =>
      fetch(`${accounts}/${path}`, {
        method: "POST",
        headers: {
          ...(user ? { "x-test-user": user } : {}),
          "content-type": "application/json",
        },
        body: body === undefined ? undefined : JSON.stringify(body),
      });
    expect((await post("backend-user")).status).toBe(401);
    // Asked as often as a page focuses, it never assigns an account.
    for (let i = 0; i < 2; i++) {
      const answer = (await (await post("backend-user", "alice")).json()) as {
        account: string | null;
        identity: { issuer: string; subject: string };
      };
      expect(answer.account).toBeNull();
      expect(answer.identity).toEqual({ issuer: "jazz:backend", subject: "alice" });
    }
    // So the guest's link to alice still goes through, and the answer names
    // the guest's account from then on.
    const secret = generateSecret();
    const { challenge } = (await (await post("backend-link/challenge", "alice")).json()) as {
      challenge: string;
    };
    const linked = await post("backend-link", "alice", {
      challenge,
      proof: signChallenge(secret, challenge),
    });
    expect(linked.status).toBe(200);
    const { account } = (await linked.json()) as { account: string };
    const after = (await (await post("backend-user", "alice")).json()) as { account: string };
    expect(after.account).toBe(account);
    // A user who signed in has an account; a GET is refused.
    await post("backend-sign-in", "bob");
    expect(
      ((await (await post("backend-user", "bob")).json()) as { account: string | null }).account,
    ).not.toBeNull();
    expect((await fetch(`${accounts}/backend-user`)).status).toBe(405);
  });

  it("links one guest to one user from two tabs at once, both to the guest's account", async () => {
    for (const guestKeyAfterLink of ["keep", "revoke"] as const) {
      const jazz = await createJazz({
        app,
        permissions,
        store: memory(),
        authenticate,
        guestKeyAfterLink,
      }).ready();
      const appId = jazz.appId;
      cleanups.push(() => jazz.close());
      const origin = await serve(jazz, { sockets: true });
      const accounts = `${origin}/jazz/apps/${appId}/accounts`;
      const post = (path: string, body?: unknown) =>
        fetch(`${accounts}/${path}`, {
          method: "POST",
          headers: { "x-test-user": "alice", "content-type": "application/json" },
          body: body === undefined ? undefined : JSON.stringify(body),
        });
      const secret = generateSecret();
      // Each tab asks for its own challenge, then both links race.
      const challenges = await Promise.all(
        [0, 1].map(
          async () =>
            ((await (await post("backend-link/challenge")).json()) as { challenge: string })
              .challenge,
        ),
      );
      const links = await Promise.all(
        challenges.map((challenge) =>
          post("backend-link", { challenge, proof: signChallenge(secret, challenge) }),
        ),
      );
      expect(
        links.map((link) => link.status),
        guestKeyAfterLink,
      ).toEqual([200, 200]);
      const [first, second] = (await Promise.all(links.map((link) => link.json()))) as {
        account: string;
      }[];
      expect(second!.account, guestKeyAfterLink).toBe(first!.account);
      const after = (await (await post("backend-user")).json()) as { account: string };
      expect(after.account, guestKeyAfterLink).toBe(first!.account);
    }
  });

  /**
   * A core behind a proxy that lets a test hold, or fail, the core's `link`
   * calls. It proxies plain requests only, not sockets: the tests that use
   * it make account calls, never sync.
   */
  async function heldCore(appId: string) {
    const core = await startLocalJazzServer({ appId, inMemory: true });
    cleanups.push(() => core.stop());
    const control: { hold?: () => Promise<void>; fail?: number } = {};
    // A held request that a failed test never released.
    let unheld: (() => void) | undefined;
    const proxy = createServer(async (request, response) => {
      if (request.url?.endsWith("/admin/accounts/link")) {
        if (control.fail) {
          response.statusCode = control.fail;
          response.end("busy");
          return;
        }
        const hold = control.hold;
        control.hold = undefined;
        if (hold) {
          await Promise.race([hold(), new Promise<void>((resolve) => (unheld = resolve))]);
        }
      }
      const target = new URL(request.url ?? "/", core.url);
      const upstream = httpRequest(
        target,
        { method: request.method, headers: { ...request.headers, host: target.host } },
        (answer) => {
          response.writeHead(answer.statusCode ?? 502, answer.headers);
          answer.pipe(response);
        },
      );
      request.pipe(upstream);
    });
    await new Promise<void>((resolve) => proxy.listen(0, "127.0.0.1", resolve));
    cleanups.push(
      () =>
        new Promise<void>((resolve) => {
          unheld?.();
          proxy.closeAllConnections();
          proxy.close(() => resolve());
        }),
    );
    const url = `http://127.0.0.1:${(proxy.address() as AddressInfo).port}`;
    return { core, url, control };
  }

  it("links a tab whose guest another tab's link revoked between founding and linking", async () => {
    const appId = randomUUID();
    const { core, url, control } = await heldCore(appId);
    const jazz = await createJazz({
      app,
      permissions,
      store: remote({ url, appId, secret: core.adminSecret }),
      authenticate,
      guestKeyAfterLink: "revoke",
    }).ready();
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: true });
    const post = (path: string, body?: unknown) =>
      fetch(`${origin}/jazz/apps/${appId}/accounts/${path}`, {
        method: "POST",
        headers: { "x-test-user": "alice", "content-type": "application/json" },
        body: body === undefined ? undefined : JSON.stringify(body),
      });
    const link = async () => {
      const { challenge } = (await (await post("backend-link/challenge")).json()) as {
        challenge: string;
      };
      return post("backend-link", { challenge, proof: signChallenge(secret, challenge) });
    };
    const secret = generateSecret();
    // Tab B's guest is founded, then its link waits at the core...
    let reached!: () => void;
    let release!: () => void;
    const atLink = new Promise<void>((resolve) => (reached = resolve));
    control.hold = () =>
      new Promise<void>((resolve) => {
        release = resolve;
        reached();
      });
    const tabB = link();
    await atLink;
    // ...while tab A links the same guest and revokes its key.
    const tabA = await link();
    expect(tabA.status).toBe(200);
    release();
    // B's link reaches the core with a revoked guest: alice already belongs
    // to the guest's account, so it is done, not refused (and B doesn't
    // revoke the key a second time when the core refused it as retired).
    const b = await tabB;
    expect(b.status).toBe(200);
    expect(((await b.json()) as { account: string }).account).toBe(
      ((await tabA.json()) as { account: string }).account,
    );
  });

  it("answers a core that fails a link for a passing reason as unavailable, and a retry links", async () => {
    const appId = randomUUID();
    const { core, url, control } = await heldCore(appId);
    const jazz = await createJazz({
      app,
      permissions,
      store: remote({ url, appId, secret: core.adminSecret }),
      authenticate,
    }).ready();
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: true });
    const post = (path: string, body?: unknown) =>
      fetch(`${origin}/jazz/apps/${appId}/accounts/${path}`, {
        method: "POST",
        headers: { "x-test-user": "alice", "content-type": "application/json" },
        body: body === undefined ? undefined : JSON.stringify(body),
      });
    const secret = generateSecret();
    const link = async () => {
      const { challenge } = (await (await post("backend-link/challenge")).json()) as {
        challenge: string;
      };
      return post("backend-link", { challenge, proof: signChallenge(secret, challenge) });
    };
    // The core down, its credential check refusing this backend mid-rotation,
    // a proxy in front of it timing out or rate-limiting: none refuses the link.
    for (const status of [503, 401, 408, 429]) {
      control.fail = status;
      const failed = await link();
      expect(failed.status, String(status)).toBe(503);
      expect(await failed.text(), String(status)).toBe("account_registry_unavailable");
    }
    control.fail = undefined;
    expect((await link()).status).toBe(200);
  });

  it("revokes the guest key on a retry when the first link's revoke didn't happen", async () => {
    const appId = randomUUID();
    const core = await startLocalJazzServer({ appId, inMemory: true });
    cleanups.push(() => core.stop());
    // Two backends on one core: the first links without revoking, as a
    // link whose revoke failed would leave it; the second revokes.
    const backend = async (guestKeyAfterLink: "keep" | "revoke") => {
      const store = remote({ url: core.url, appId, secret: core.adminSecret });
      const jazz = await createJazz({
        app,
        permissions,
        store,
        authenticate,
        guestKeyAfterLink,
      }).ready();
      cleanups.push(() => jazz.close());
      const origin = await serve(jazz, { sockets: true });
      const accounts = `${origin}/jazz/apps/${appId}/accounts`;
      const post = (path: string, user: string, body?: unknown) =>
        fetch(`${accounts}/${path}`, {
          method: "POST",
          headers: { "x-test-user": user, "content-type": "application/json" },
          body: body === undefined ? undefined : JSON.stringify(body),
        });
      return async (user: string, secret: string) => {
        const { challenge } = (await (await post("backend-link/challenge", user)).json()) as {
          challenge: string;
        };
        return post("backend-link", user, { challenge, proof: signChallenge(secret, challenge) });
      };
    };
    const keeps = await backend("keep");
    const revokes = await backend("revoke");
    const secret = generateSecret();

    const first = await keeps("alice", secret);
    expect(first.status).toBe(200);
    const { account } = (await first.json()) as { account: string };
    // The retry links nothing new, and retires the key.
    const retried = await revokes("alice", secret);
    expect(retried.status).toBe(200);
    expect(((await retried.json()) as { account: string }).account).toBe(account);
    expect((await keeps("bob", secret)).status).toBe(409);
    // A retry whose response was lost still answers alice, and only her.
    expect((await revokes("alice", secret)).status).toBe(200);
    expect((await revokes("bob", secret)).status).toBe(409);
  }, 60_000);

  it("refuses a socket for a revoked identity as an auth failure, not an outage", async () => {
    const appId = randomUUID();
    const core = await startLocalJazzServer({ appId, inMemory: true });
    cleanups.push(() => core.stop());
    const store = remote({ url: core.url, appId, secret: core.adminSecret });
    const jazz = await createJazz({ app, permissions, store, authenticate }).ready();
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: true });

    // The app's own session still names alice, but her identity is revoked
    // at the core.
    const alice = { issuer: "jazz:backend", subject: "alice" };
    const admin = (operation: string, body: unknown) =>
      fetch(`${core.url}/apps/${appId}/admin/accounts/${operation}`, {
        method: "POST",
        headers: { "X-Jazz-Admin-Secret": core.adminSecret, "content-type": "application/json" },
        body: JSON.stringify(body),
      });
    expect((await admin("login-or-register", { identity: alice })).status).toBe(200);
    expect((await admin("revoke", { approver: alice, target: alice })).status).toBe(204);

    const socket = new NodeWebSocket(`${origin.replace("http", "ws")}/jazz/apps/${appId}/ws`, {
      headers: { "x-test-user": "alice" },
    } as never);
    socket.binaryType = "arraybuffer";
    socket.addEventListener("open", () =>
      socket.send(
        JSON.stringify({
          peer_identity: "revoked",
          backend_session: { issuer: alice.issuer, user_id: "alice" },
        }),
      ),
    );
    const messages: string[] = [];
    socket.addEventListener("message", (event) =>
      messages.push(new TextDecoder().decode(new Uint8Array(event.data as ArrayBuffer))),
    );
    const closed = await new Promise<{ code: number; reason: string }>((resolve) =>
      socket.addEventListener("close", (event) =>
        resolve({ code: event.code, reason: event.reason }),
      ),
    );
    // A refusal the client must not retry: an auth-failed frame, then 1008.
    // An outage (core unreachable, 5xx) closes 1011 and the client retries.
    expect(closed).toEqual({ code: 1008, reason: "sign-in refused" });
    expect(messages).toHaveLength(1);
    expect(messages[0]).toContain("session refused: identity_not_authorized");
  }, 60_000);

  it("syncs over the HTTP stream when the host can't upgrade sockets", async () => {
    const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    const appId = jazz.appId;
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
      .insert(app.todos, { title: "over the stream", owner: alice.session!.user! })
      .wait({ tier: "global" });
    const seen = await jazz.db.all(app.todos.where({ id: row.id }));
    expect(seen.map((todo) => todo.title)).toEqual(["over the stream"]);
    expect(requestPaths.some((path) => path.endsWith(`/apps/${appId}/stream`))).toBe(true);

    // A session id alone (it can end up in logs) doesn't let anyone act in
    // the session, nor end it, not even with alice's cookie; and the key
    // with another user's cookie doesn't either.
    const sessionPath = requestPaths.find((path) => /\/stream\/[^/]+$/.test(path))!;
    const key = streamKeys.get(sessionPath)!;
    expect(key).toBeTruthy();
    for (const method of ["POST", "DELETE"]) {
      expect(await statusOf(`${origin}${sessionPath}`, method, { "x-test-user": "bob" })).toBe(403);
      expect(await statusOf(`${origin}${sessionPath}`, method, { "x-test-user": "alice" })).toBe(
        403,
      );
      expect(
        await statusOf(`${origin}${sessionPath}`, method, {
          "x-test-user": "bob",
          "x-jazz-stream-key": key,
        }),
      ).toBe(403);
    }
    // A session this process doesn't hold says why.
    const unknown = await fetch(`${origin}/jazz/apps/${appId}/stream/nope`, { method: "POST" });
    expect(unknown.status).toBe(404);
    expect(await unknown.text()).toContain("sticky routing");
    // Alice's session still works after both attempts.
    const later = await alice.db
      .insert(app.todos, { title: "still over the stream", owner: alice.session!.user! })
      .wait({ tier: "global" });
    expect((await jazz.db.all(app.todos.where({ id: later.id }))).length).toBe(1);
  }, 60_000);

  it("sends a failed socket to the HTTP stream only through handlers that can't upgrade", async () => {
    const appId = randomUUID();
    const marker = async (response: Promise<Response>) =>
      (await response).headers.get("x-jazz-stream");
    const probe = () => new Request(`http://app.test/jazz/apps/${appId}/ws`);

    const plain = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    cleanups.push(() => plain.close());
    const previous = process.env.VERCEL;
    process.env.VERCEL = "1";
    try {
      // On Vercel, only next() upgrades: a Hono or plain fetch handler there
      // can't, so its clients need the stream.
      expect(await marker(plain.handle(probe()))).toBe("1");
      expect(await marker(plain.next().GET(probe()))).toBeNull();
    } finally {
      if (previous === undefined) delete process.env.VERCEL;
      else process.env.VERCEL = previous;
    }
    expect(await marker(plain.next().GET(probe()))).toBe("1"); // next dev

    // A host that upgrades on its own and hands sockets to acceptSocket.
    const own = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate,
      upgradesSockets: true,
    }).ready();
    cleanups.push(() => own.close());
    expect(
      await marker(own.handle(new Request(`http://app.test/jazz/apps/${own.appId}/ws`))),
    ).toBeNull();

    // An entry that isn't an origin (it would parse as the opaque "null").
    for (const entry of ["localhost:3000", "app.example.com:443", "not a url"]) {
      expect(() =>
        createJazz({ app, permissions, store: memory(), allowedOrigins: [entry] }),
      ).toThrow(/is not an origin/);
    }
  }, 60_000);

  it("opens a sqlite directory in one core at a time, and not on serverless hosts", async () => {
    const dir = await mkdtemp(join(tmpdir(), "jazz-lock-"));
    cleanups.push(() => rm(dir, { recursive: true, force: true }));
    // A lock file left by a process that died (under Docker often our own
    // pid, back after a restart) holds nothing: only the OS lock counts.
    await writeFile(join(dir, "jazz.lock"), `process ${process.pid}`);
    // In one process, every createJazz for the directory is the same Jazz.
    const first = createJazz({ app, permissions, store: sqlite({ path: dir }) });
    expect(createJazz({ app, permissions, store: sqlite({ path: `${dir}/.` }) })).toBe(first);
    const appId = (await first.ready()).appId;
    await first.close();

    // Another core holding it (in production, another process) is refused,
    // with why and what to do.
    const other = await JazzCore.open({
      appId,
      backendSecret: randomUUID(),
      adminSecret: randomUUID(),
      dataDir: dir,
      blobs: "files",
    });
    const refused = createJazz({ app, permissions, store: sqlite({ path: dir }) });
    expect(refused).not.toBe(first);
    await expect(refused.ready()).rejects.toThrow(/is already open.*another process/s);
    await other.close();
    // Free again, the same Jazz opens on its next use after the backoff.
    const now = Date.now.bind(Date);
    const spy = vi.spyOn(Date, "now").mockImplementation(() => now() + 2_000);
    await refused.ready();
    spy.mockRestore();
    await refused.close();

    const previous = process.env.VERCEL;
    process.env.VERCEL = "1";
    try {
      await expect(
        createJazz({ app, permissions, store: sqlite({ path: dir }) }).ready(),
      ).rejects.toThrow(/Use vercel\(\), whose local store runs sqlite\(\) in dev, or remote\(/);
    } finally {
      if (previous === undefined) delete process.env.VERCEL;
      else process.env.VERCEL = previous;
    }
  }, 60_000);

  it("keeps data in the sqlite directory across restarts", async () => {
    const dir = await mkdtemp(join(tmpdir(), "jazz-embedded-"));
    cleanups.push(() => rm(dir, { recursive: true, force: true }));

    // The core names its own app and keeps that id with its data.
    const first = await createJazz({ app, permissions, store: sqlite({ path: dir }) }).ready();
    const appId = first.appId;
    expect(appId).toMatch(/^[0-9a-f-]{36}$/);
    const inserted = await first.db
      .insert(app.todos, { title: "kept", owner: randomUUID() })
      .wait({ tier: "global" });
    await first.close();

    const second = await createJazz({ app, permissions, store: sqlite({ path: dir }) }).ready();
    cleanups.push(() => second.close());
    expect(second.appId).toBe(appId);
    // The store keeps its own id: naming one is refused, not ignored.
    expect(() =>
      sqlite({ path: dir, appId: randomUUID() } as unknown as Parameters<typeof sqlite>[0]),
    ).toThrow(/appId is not an option.*jazz-alpha59-alpha60-import/);
    expect(() => memory({ appId: randomUUID() } as never)).toThrow(/appId is not an option/);
    // vercel() too, when called: in dev its local store would never read it.
    expect(() => vercel({ appId: randomUUID() } as never)).toThrow(
      /vercel\(\): appId is not an option/,
    );
    const rows = await second.db.all(app.todos.where({ id: inserted.id }));
    expect(rows.map((todo) => todo.title)).toEqual(["kept"]);
  }, 60_000);

  it("runs a sqlite store as the alpha.59 app its import kept", async () => {
    const dir = await mkdtemp(join(tmpdir(), "jazz-imported-"));
    cleanups.push(() => rm(dir, { recursive: true, force: true }));
    // What `jazz-alpha59-alpha60-import import --app-id` leaves beside the
    // data: the id exactly as given, case included.
    const alpha59 = "0A590000-0000-4000-8000-000000000059";
    await writeFile(join(dir, "app-id"), `${alpha59}\n`);
    const jazz = await createJazz({ app, permissions, store: sqlite({ path: dir }) }).ready();
    cleanups.push(() => jazz.close());
    expect(jazz.appId).toBe(alpha59);
    await jazz.db.insert(app.todos, { title: "after import", owner: randomUUID() }).wait({
      tier: "global",
    });
  }, 60_000);

  it("hands writes to the sqlite core before close, even ones it hasn't confirmed", async () => {
    const dir = await mkdtemp(join(tmpdir(), "jazz-embedded-"));
    cleanups.push(() => rm(dir, { recursive: true, force: true }));

    const first = createJazz({ app, permissions, store: sqlite({ path: dir }) });
    // Resolved at "local": the backend's own client has it, the core may not yet.
    const written = await Promise.all(
      Array.from({ length: 5 }, (_, i) =>
        first.db.insert(app.todos, { title: `pending ${i}`, owner: randomUUID() }).wait({
          tier: "local",
        }),
      ),
    );
    await first.close();

    const second = createJazz({ app, permissions, store: sqlite({ path: dir }) });
    cleanups.push(() => second.close());
    const rows = await second.db.all(app.todos, { tier: "remote" });
    expect(rows.map((todo) => todo.id).sort()).toEqual(written.map((todo) => todo.id).sort());
  }, 60_000);

  it("keeps a sqlite store's large values as files, and keeps that choice", async () => {
    const dir = await mkdtemp(join(tmpdir(), "jazz-blobs-"));
    cleanups.push(() => rm(dir, { recursive: true, force: true }));
    const title = "large ".repeat(60_000);

    const first = await createJazz({ app, permissions, store: sqlite({ path: dir }) }).ready();
    const inserted = await first.db
      .insert(app.todos, { title, owner: randomUUID() })
      .wait({ tier: "global" });
    // Settled leaves move out of the database on later ticks.
    const blobFiles = async () =>
      (await readdir(join(dir, "blobs"), { recursive: true })).filter((name) =>
        /^[0-9a-f]{2}[\\/][0-9a-f]{64}$/.test(name),
      );
    await expect.poll(blobFiles, { timeout: 20_000 }).not.toHaveLength(0);
    await first.close();

    const second = await createJazz({ app, permissions, store: sqlite({ path: dir }) }).ready();
    const rows = await second.db.all(app.todos.where({ id: inserted.id }));
    expect(rows.map((todo) => todo.title)).toEqual([title]);
    await second.close();

    // Back into the database file is not supported yet.
    await expect(
      createJazz({ app, permissions, store: sqlite({ path: dir, blobs: "sqlite" }) }).ready(),
    ).rejects.toThrow(/not supported yet/);
  }, 60_000);

  it("moves a sqlite store's large values out of the database file", async () => {
    const dir = await mkdtemp(join(tmpdir(), "jazz-blobs-move-"));
    cleanups.push(() => rm(dir, { recursive: true, force: true }));
    const title = "moved ".repeat(60_000);

    const inFile = await createJazz({
      app,
      permissions,
      store: sqlite({ path: dir, blobs: "sqlite" }),
    }).ready();
    const inserted = await inFile.db
      .insert(app.todos, { title, owner: randomUUID() })
      .wait({ tier: "global" });
    await inFile.close();
    await expect(readdir(join(dir, "blobs"))).rejects.toThrow();

    const custom = join(dir, "elsewhere");
    const moved = await createJazz({
      app,
      permissions,
      store: sqlite({ path: dir, blobs: files({ dir: custom }) }),
    }).ready();
    cleanups.push(() => moved.close());
    await expect
      .poll(async () => (await readdir(custom, { recursive: true })).length, { timeout: 20_000 })
      .toBeGreaterThan(0);
    const rows = await moved.db.all(app.todos.where({ id: inserted.id }));
    expect(rows.map((todo) => todo.title)).toEqual([title]);
  }, 60_000);

  it("fronts a remote core: browsers reach only the backend, the core trusts it", async () => {
    const appId = randomUUID();
    const core = await startLocalJazzServer({ appId, inMemory: true });
    cleanups.push(() => core.stop());
    // The app's one secret: the core's admin secret.
    const store = remote({ url: core.url, appId, secret: core.adminSecret });
    const jazz = await createJazz({ app, permissions, store, authenticate }).ready();
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: true });

    const alice = await userSession(appId, origin, "alice");
    const row = await alice.db
      .insert(app.todos, { title: "through the backend", owner: alice.session!.user! })
      .wait({ tier: "global" });

    // A second backend on the same core (another instance, after a restart)
    // sees the same data and the same accounts.
    const other = await createJazz({ app, permissions, store, authenticate }).ready();
    cleanups.push(() => other.close());
    const seen = await other.db.all(app.todos.where({ id: row.id }));
    expect(seen.map((todo) => todo.title)).toEqual(["through the backend"]);
    const asAlice = await other.forReqUser(
      new Request("http://app/x", { headers: { "x-test-user": "alice" } }),
    );
    expect((await asAlice.all(app.todos)).map((todo) => todo.title)).toEqual([
      "through the backend",
    ]);
    const asBob = await other.forReqUser(
      new Request("http://app/x", { headers: { "x-test-user": "bob" } }),
    );
    expect(await asBob.all(app.todos)).toEqual([]);

    // A guest whose client names no app: the backend vouches for the
    // endpoint its proof is for, and the remote core founds its account.
    // (From a browser nobody signed in on: a signed-in request is the
    // user's, never a guest's.)
    const guestOrigin = await serve(jazz, { sockets: true });
    const guestSession = await createJazzClient({
      app,
      permissions,
      serverUrl: `${guestOrigin}/jazz`,
      driver: { type: "memory" },
      initial: "local-first",
    });
    cleanups.push(() => guestSession.close());
    const guest = guestSession.getSnapshot().client!;
    const made = await guest.db
      .insert(app.todos, { title: "as a guest", owner: guest.session!.user! })
      .wait({ tier: "global" });
    expect((await jazz.db.all(app.todos.where({ id: made.id }))).map((todo) => todo.title)).toEqual(
      ["as a guest"],
    );
  }, 60_000);

  it("reconnects to a remote core that restarts, and still closes", async () => {
    const appId = randomUUID();
    const dataDir = await mkdtemp(join(tmpdir(), "jazz-remote-restart-"));
    cleanups.push(() => rm(dataDir, { recursive: true, force: true }));
    const adminSecret = `restart-admin-${randomUUID().slice(0, 8)}`;
    const backendSecret = `restart-backend-${randomUUID().slice(0, 8)}`;
    const start = (port = 0) =>
      startLocalJazzServer({ appId, port, dataDir, adminSecret, backendSecret });
    let core = await start();
    cleanups.push(() => core.stop());
    const port = core.port;
    const jazz = await createJazz({
      app,
      permissions,
      store: remote({ url: core.url, appId, secret: adminSecret }),
      authenticate,
    }).ready();
    let closed = false;
    cleanups.push(() => (closed ? Promise.resolve() : jazz.close()));
    const asAlice = () =>
      jazz.forReqUser(new Request("http://app/x", { headers: { "x-test-user": "alice" } }));
    const alice = await jazz.accountFor(
      new Request("http://app/x", { headers: { "x-test-user": "alice" } }),
    );
    await (
      await asAlice()
    )
      .insert(app.todos, { title: "before", owner: alice! })
      .wait({ tier: "global" });

    // The core goes away and comes back on the same address and data.
    await core.stop();
    core = await start(port);

    const within = <T>(promise: Promise<T>, what: string) =>
      Promise.race([
        promise,
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error(`${what} hung after the core restarted`)), 20_000),
        ),
      ]);
    const db = await within(asAlice(), "forReqUser");
    await within(
      db.insert(app.todos, { title: "after", owner: alice! }).wait({ tier: "global" }),
      "a global write",
    );
    const titles = await within(db.all(app.todos, { tier: "remote" }), "a remote read");
    expect(titles.map((todo) => todo.title).sort()).toEqual(["after", "before"]);

    // The core sees the write: it reached the restarted core, not a cache.
    const other = await createJazz({
      app,
      permissions,
      store: remote({ url: core.url, appId, secret: adminSecret }),
      authenticate,
    }).ready();
    cleanups.push(() => other.close());
    expect((await other.db.all(app.todos)).map((todo) => todo.title).sort()).toEqual([
      "after",
      "before",
    ]);

    await within(jazz.close(), "close()");
    closed = true;
  }, 90_000);

  it("closes while its remote core is down, with a write still pending", async () => {
    const appId = randomUUID();
    const core = await startLocalJazzServer({ appId, inMemory: true });
    cleanups.push(() => core.stop());
    const jazz = await createJazz({
      app,
      permissions,
      store: remote({ url: core.url, appId, secret: core.adminSecret }),
      authenticate,
    }).ready();
    let closed = false;
    cleanups.push(() => (closed ? Promise.resolve() : jazz.close()));
    const request = new Request("http://app/x", { headers: { "x-test-user": "alice" } });
    const alice = await jazz.accountFor(request);
    const db = await jazz.forReqUser(request);
    await db.insert(app.todos, { title: "reached", owner: alice! }).wait({ tier: "global" });

    await core.stop();
    db.insert(app.todos, { title: "stranded", owner: alice! });

    // A SIGTERM handler's close(): it gives pending writes their time, then
    // ends instead of waiting for a core that is not coming back.
    const started = Date.now();
    await Promise.race([
      jazz.close(),
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error("close() hung with the core down")), 25_000),
      ),
    ]);
    closed = true;
    expect(Date.now() - started).toBeLessThan(25_000);
  }, 60_000);

  it("serves each deployment's sessions by its own permissions, side by side", async () => {
    const appId = randomUUID();
    const core = await startLocalJazzServer({ appId, inMemory: true });
    cleanups.push(() => core.stop());
    const store = remote({ url: core.url, appId, secret: core.adminSecret });
    const noInserts = s.definePermissions(app, ({ policy, session }) => {
      policy.todos.allowRead.where({ owner: session.user });
      policy.todos.allowInsert.never();
      policy.todos.allowUpdate.never();
      policy.todos.allowDelete.never();
    });
    // The current deployment, then a newer one that boots last: neither
    // takes over the other's sessions.
    const current = await createJazz({ app, permissions, store, authenticate }).ready();
    cleanups.push(() => current.close());
    const newer = await createJazz({ app, permissions: noInserts, store, authenticate }).ready();
    cleanups.push(() => newer.close());
    const [onCurrent, onNewer] = [
      await serve(current, { sockets: true }),
      await serve(newer, { sockets: true }),
    ];

    // Local-first guests too: each backend pins its guests' sessions.
    const guestOf = async (origin: string) => {
      const session = await createJazzClient({
        app,
        permissions,
        serverUrl: `${origin}/jazz`,
        driver: { type: "memory" },
        initial: "local-first",
      });
      cleanups.push(() => session.close());
      return session.getSnapshot().client!;
    };
    const guestOnCurrent = await guestOf(onCurrent);
    const guestRow = await guestOnCurrent.db
      .insert(app.todos, {
        title: "a guest's, by the current deployment",
        owner: guestOnCurrent.session!.user!,
      })
      .wait({ tier: "global" });
    const guestOnNewer = await guestOf(onNewer);
    await expect(
      guestOnNewer.db
        .insert(app.todos, {
          title: "a guest's, by the newer deployment",
          owner: guestOnNewer.session!.user!,
        })
        .wait({ tier: "global" }),
    ).rejects.toThrow();

    const alice = await userSession(appId, onCurrent, "alice");
    const row = await alice.db
      .insert(app.todos, {
        title: "by the current deployment",
        owner: alice.session!.user!,
      })
      .wait({ tier: "global" });
    // The browser's own permissions allow the insert; the core refuses it
    // by the newer deployment's.
    const bob = await userSession(appId, onNewer, "bob");
    await expect(
      bob.db
        .insert(app.todos, { title: "by the newer deployment", owner: bob.session!.user! })
        .wait({ tier: "global" }),
    ).rejects.toThrow();
    // The backend's own scoped writes too.
    const asBob = await newer.forReqUser(
      new Request("http://app/x", { headers: { "x-test-user": "bob" } }),
    );
    expect(await asBob.all(app.todos)).toEqual([]);

    // Rolling back: the current deployment boots again, and serves as before.
    const rolledBack = await createJazz({ app, permissions, store, authenticate }).ready();
    cleanups.push(() => rolledBack.close());
    const asAlice = await rolledBack.forReqUser(
      new Request("http://app/x", { headers: { "x-test-user": "alice" } }),
    );
    const again = await asAlice
      .insert(app.todos, { title: "after the rollback", owner: alice.session!.user! })
      .wait({ tier: "global" });
    expect((await rolledBack.db.all(app.todos)).map((todo) => todo.id).sort()).toEqual(
      [guestRow.id, row.id, again.id].sort(),
    );
  }, 60_000);

  it("starts a deployment that switches a table to latest-only only when it says so", async () => {
    const appId = randomUUID();
    const core = await startLocalJazzServer({ appId, inMemory: true });
    cleanups.push(() => core.stop());
    const store = remote({ url: core.url, appId, secret: core.adminSecret });
    const latest = s.defineApp(
      {
        todos: s.table({ title: s.string(), owner: s.uuid() }, {}).history("latest"),
      },
      { migrations: "none" },
    );
    const latestPermissions = s.definePermissions(latest, ({ policy, session }) => {
      policy.todos.allowRead.where({ owner: session.user });
      policy.todos.allowInsert.where({ owner: session.user });
      policy.todos.allowUpdate.never();
      policy.todos.allowDelete.never();
    });
    const full = await createJazz({ app, permissions, store, authenticate }).ready();
    cleanups.push(() => full.close());

    const refused = createJazz({
      app: latest,
      permissions: latestPermissions,
      store,
      authenticate,
    });
    cleanups.push(() => refused.close());
    await expect(refused.ready()).rejects.toThrow(
      /switches table `todos` to latest-only[\s\S]*allowDroppingHistory: true/,
    );
    const confirmed = await createJazz({
      app: latest,
      permissions: latestPermissions,
      store,
      authenticate,
      allowDroppingHistory: true,
    }).ready();
    cleanups.push(() => confirmed.close());
    // A latest-only version is admitted now: starting it again needs no
    // confirmation.
    const again = await createJazz({
      app: latest,
      permissions: latestPermissions,
      store,
      authenticate,
    }).ready();
    cleanups.push(() => again.close());
  }, 60_000);

  it.each([
    ["has no route for bundles", 404, /predates per-deployment permissions bundles/],
    ["publishes no bundle", 200, /did not publish a permissions bundle/],
  ] as const)("refuses a core that %s", async (_label, status, error) => {
    const appId = randomUUID();
    const core = await startLocalJazzServer({ appId, inMemory: true });
    cleanups.push(() => core.stop());
    // An older core: everything but the `/bundles` route it never had.
    const older = createServer((request, response) => {
      if (request.url?.endsWith("/admin/bundles")) {
        response.statusCode = status;
        response.setHeader("content-type", "application/json");
        response.end(
          status === 200
            ? JSON.stringify({ changed: true, published: { schemas: [], migrations: [] } })
            : "",
        );
        return;
      }
      const target = new URL(request.url ?? "/", core.url);
      const upstream = httpRequest(
        target,
        { method: request.method, headers: { ...request.headers, host: target.host } },
        (answer) => {
          response.writeHead(answer.statusCode ?? 502, answer.headers);
          answer.pipe(response);
        },
      );
      request.pipe(upstream);
    });
    await new Promise<void>((resolve) => older.listen(0, "127.0.0.1", resolve));
    cleanups.push(() => new Promise<void>((resolve) => older.close(() => resolve())));
    const url = `http://127.0.0.1:${(older.address() as AddressInfo).port}`;

    await expect(
      createJazz({
        app,
        permissions,
        store: remote({ url, appId, secret: core.adminSecret }),
      }).ready(),
    ).rejects.toThrow(error);
    // The real core published no bundle, so no session can pin to it.
    const listed = await fetch(`${core.url}/apps/${appId}/admin/bundles`, {
      headers: { "X-Jazz-Admin-Secret": core.adminSecret },
    });
    expect(((await listed.json()) as { bundles: unknown[] }).bundles).toEqual([]);
  });

  it("presents the backend credential on every request and socket to a remote core", async () => {
    const appId = randomUUID();
    const core = await startLocalJazzServer({ appId, inMemory: true });
    cleanups.push(() => core.stop());

    // Not the admin secret (the backend secret included): nothing opens.
    for (const secret of ["not-the-secret", core.backendSecret]) {
      await expect(
        createJazz({ app, permissions, store: remote({ url: core.url, appId, secret }) }).ready(),
      ).rejects.toThrow();
    }
    expect(() => remote({ url: core.url, appId, secret: "" })).toThrow(/admin secret/);
    // Untyped callers of the old shape hear about the rename.
    expect(() =>
      remote({ url: core.url, appId, adminSecret: core.adminSecret } as unknown as {
        url: string;
        appId: string;
        secret: string;
      }),
    ).toThrow(/replaced by one `secret`/);
    expect(() => remote({ url: core.url, appId: "", secret: core.adminSecret })).toThrow(
      /appId must name the app/,
    );
    // createJazz names no app of its own any more: remote() does.
    expect(() =>
      createJazz({
        appId,
        app,
        permissions,
        store: memory(),
      } as unknown as Parameters<typeof createJazz>[0]),
    ).toThrow(/appId moved to the store/);

    // A credential hook is asked fresh for each request and socket.
    let asked = 0;
    const store = remote({
      url: core.url,
      appId,
      backendCredential: async () => {
        asked++;
        return core.adminSecret;
      },
    });
    const jazz = await createJazz({ app, permissions, store, authenticate }).ready();
    cleanups.push(() => jazz.close());
    const opened = asked;
    expect(opened).toBeGreaterThan(1);
    const origin = await serve(jazz, { sockets: true });

    const alice = await userSession(appId, origin, "alice");
    await alice.db
      .insert(app.todos, { title: "vouched", owner: alice.session!.user! })
      .wait({ tier: "global" });
    expect(asked).toBeGreaterThan(opened);

    // A browser can't state an access of its own: the schema list needs
    // admin, and the backend relays public routes with no access.
    const forged = await fetch(`${origin}/jazz/apps/${appId}/schemas`, {
      headers: {
        "X-Jazz-Backend-Credential": core.adminSecret,
        "X-Jazz-Backend-Access": "admin",
      },
    });
    expect(forged.status).toBe(401);

    // Nor on a sync socket: claiming the system identity with a forged
    // credential and no secret of its own fails admission (after an auth
    // failure frame, the socket closes).
    const socket = new WebSocket(`${origin.replace(/^http/, "ws")}/jazz/apps/${appId}/ws`, {
      headers: {
        "X-Jazz-Backend-Credential": core.adminSecret,
        "X-Jazz-Backend-Access": "admin",
      },
    } as unknown as string[]);
    socket.binaryType = "arraybuffer";
    const outcome = await new Promise<string>((resolve) => {
      socket.addEventListener("open", () =>
        socket.send(JSON.stringify({ peer_identity: `["urn:jazz:system","system"]`, auth: {} })),
      );
      socket.addEventListener("close", (event) => resolve(`close ${event.code} ${event.reason}`));
    });
    expect(outcome).toBe("close 1008 not signed in");
    socket.close();
  }, 60_000);
});
