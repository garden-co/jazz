import { spawn } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import { mkdtemp, readdir, rm, writeFile } from "node:fs/promises";
import {
  Agent as HttpAgent,
  createServer,
  request as httpRequest,
  type IncomingMessage,
  type Server,
} from "node:http";
import {
  createServer as createHttp2Server,
  createSecureServer as createSecureHttp2Server,
  type Http2SecureServer,
  type Http2Server,
} from "node:http2";
import { createRequire } from "node:module";
import type { AddressInfo, Socket } from "node:net";
import { tmpdir } from "node:os";
import type { Duplex } from "node:stream";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { generateSecret, identityOf, signChallenge, signToken } from "../local-first-auth/index.js";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { JazzCore } from "jazz-napi";
import { afterEach, describe, expect, it, vi } from "vitest";
import { appArtifact, defineShippedApp } from "../testing/app-artifact.js";
import type { CompiledPermissionsMap } from "../schema-permissions.js";
import { decodeWebSocketFrameBatch } from "../runtime/native-runtime/websocket.js";
import { schema as s } from "../index.js";
import { startLocalJazzServer } from "../dev/dev-server.js";
import {
  acceptDeviceAccounts,
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
  type WsLikeSocket,
} from "./index.js";
import { localFirstAccountId } from "../accounts/local-first.js";
import { mapAuthReason } from "../runtime/auth-state.js";
import { requestBackendSignIn, requestBackendUser } from "../accounts/registry-client.js";
import { signedInIdentity } from "../session/follow-user.js";
import {
  encodeWebSocketFrameBatch,
  encodeWireClientHello,
} from "../runtime/native-runtime/websocket.js";

const todosDefinition = {
  todos: s.table({ title: s.string(), owner: s.uuid() }, {}),
};
const { app, permissions } = await defineShippedApp(todosDefinition, (app) =>
  s.definePermissions(app, ({ policy, session }) => {
    policy.todos.allowRead.where({ owner: session.user });
    policy.todos.allowInsert.where({ owner: session.user });
    policy.todos.allowUpdate.never();
    policy.todos.allowDelete.never();
  }),
);

/**
 * `app`, shipping a jazz.app.json that holds `other` permissions, for a test
 * that serves it with them: a backend refuses a file whose permissions differ.
 */
async function appWith(other: CompiledPermissionsMap): Promise<typeof app> {
  return s.defineApp(todosDefinition, {
    artifact: await appArtifact(todosDefinition, { permissions: other }),
  }) as unknown as typeof app;
}

const NodeWebSocket = globalThis.WebSocket;
const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
});

/** The test app's own auth: the user named in a header, else a local-first guest. */
function authenticate(request: Request) {
  const userId = request.headers.get("x-test-user");
  return userId ? { userId, expiresAt: inAnHour() } : acceptDeviceAccounts(request);
}

/** An `expiresAt` an hour from now, in Unix seconds. */
function inAnHour() {
  return Math.floor(Date.now() / 1000) + 3600;
}

/** An `expiresAt` `seconds` from now, in Unix seconds (fractional is fine). */
function inSeconds(seconds: number) {
  return Date.now() / 1000 + seconds;
}

/** Sample `read()` every 10 ms while `during` runs; resolves with every distinct value seen. */
async function watchWhile<T>(read: () => T, during: Promise<unknown>): Promise<T[]> {
  const seen: T[] = [];
  const note = () => {
    const value = read();
    if (!seen.some((other) => JSON.stringify(other) === JSON.stringify(value))) seen.push(value);
  };
  const timer = setInterval(note, 10);
  try {
    note();
    await during;
  } finally {
    clearInterval(timer);
  }
  note();
  return seen;
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
  if (options.sockets) await jazz.attach(server);
  // attach() keeps Jazz's sockets from other upgrade listeners: count them
  // as the server emits them.
  const onUpgrade = options.onUpgrade;
  if (onUpgrade) {
    const emit = server.emit;
    server.emit = function (this: Server, event: string | symbol, ...args: unknown[]) {
      if (event === "upgrade") onUpgrade();
      return emit.apply(this, [event, ...args] as Parameters<typeof emit>);
    } as typeof server.emit;
  }
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
  await session.refresh();
  const { client, status, error } = session.getSnapshot();
  if (status !== "ready") throw error ?? new Error(status);
  return client!;
}

const randomBytesBase64 = () => randomBytes(16).toString("base64");

// Hono's `serve()` returns any of these, and `attach()` takes each.
export type AttachesHonoServers = [Server | Http2Server | Http2SecureServer] extends [
  Parameters<Jazz["attach"]>[0],
]
  ? true
  : never;
const attachesHonoServers: AttachesHonoServers = true;
void attachesHonoServers;

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
    // Bob never signed in: looking him up or acting as him makes no account.
    const bobRequest = () => new Request("http://app/x", { headers: { "x-test-user": "bob" } });
    await expect(jazz.accountFor(bobRequest())).resolves.toBeNull();
    await expect(jazz.forReqUser(bobRequest())).rejects.toMatchObject({
      name: "NoJazzAccountError",
      code: "no_account",
      userId: "bob",
    });
    await expect(jazz.withAttribution(bobRequest())).rejects.toMatchObject({ code: "no_account" });
    await expect(jazz.accountFor(bobRequest())).resolves.toBeNull();
    // The backend creates his account on purpose, and then acts as him.
    const bobAccount = await jazz.createAccountFor("bob");
    await expect(jazz.createAccountFor("bob")).resolves.toBe(bobAccount);
    await expect(jazz.accountFor(bobRequest())).resolves.toBe(bobAccount);
    const asBob = await jazz.forReqUser(bobRequest());
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
        .insert(app.todos, { title: "synced", owner: alice.session!.user! })
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
        // The host prints "<port> <app id>" once it serves; the store's
        // "jazz: store …" line comes before it.
        let out = "";
        child.stdout!.on("data", (chunk: Buffer) => {
          out += `${chunk}`;
          const line = out.split("\n").find((l) => /^\d+ \S+$/.test(l.trim()));
          if (line) resolve(line.trim());
        });
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

  it("keeps its own link when synchronous work holds the event loop as it opens", async () => {
    const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    cleanups.push(() => jazz.close());
    const owner = randomUUID();
    // The first write opens the backend's link to its core; the loop then
    // holds the event loop past a network peer's 2 s handshake deadline
    // before the client could send its Hello.
    jazz.db.insert(app.todos, { title: "first", owner });
    const until = performance.now() + 3_000;
    while (performance.now() < until) {
      // A burst of synchronous work, as a bulk insert is.
    }
    const rows = await jazz.db.all(app.todos.where({ owner }));
    expect(rows.map((row) => row.title)).toEqual(["first"]);
  });

  it("answers a signed-out request with an authentication error, not a plain one", async () => {
    const failure = new Error("auth service down");
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: (request) => {
        if (request.headers.has("x-fail")) throw failure;
        return request.headers.get("x-test-user")
          ? { userId: "alice", expiresAt: inAnHour() }
          : null;
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
    // What express.json() and express.text() do before the app's handlers:
    // read the stream whole and leave the result on `req.body`.
    const parse = (req: IncomingMessage & { body?: unknown }, done: () => void) => {
      const chunks: Buffer[] = [];
      req.on("data", (chunk: Buffer) => chunks.push(chunk));
      req.on("end", () => {
        const raw = Buffer.concat(chunks);
        const type = req.headers["content-type"];
        req.body =
          type === "application/json"
            ? JSON.parse(`${raw}`)
            : type === "text/plain"
              ? `${raw}`
              : raw;
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

    // A JSON body express.json() parsed serves Jazz's JSON routes as JSON:
    // the link route gets to judge it (no proof here), not a parser error.
    const linked = await fetch(`${origin}/jazz/apps/${jazz.appId}/accounts/backend-link`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-test-user": "alice" },
      body: JSON.stringify({ challenge: "c", proof: "p" }),
    });
    expect(linked.status).toBeGreaterThanOrEqual(400);
    expect(linked.status).toBeLessThan(500);
    expect(await linked.text()).not.toBe("request_body_already_read");

    // A Jazz route whose body another parser took (as a string) fails
    // plainly, naming the fix, instead of with a locked-stream error.
    const errors = vi.spyOn(console, "error").mockImplementation(() => undefined);
    try {
      const taken = await fetch(`${origin}/jazz/auth/sign-in`, {
        method: "POST",
        headers: { "content-type": "text/plain" },
        body: "{}",
      });
      expect(taken.status).toBe(500);
      // A code the browser's client reports, not a bare "Internal error".
      expect(await taken.text()).toBe("request_body_already_read");
      expect(errors.mock.calls.flat().join(" ")).toMatch(
        /mount jazz.nodeHandler before body parsers/,
      );
    } finally {
      errors.mockRestore();
    }
  });

  it("signs a browser in behind a host's JSON body parser", async () => {
    const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    cleanups.push(() => jazz.close());
    const parsed: string[] = [];
    let strict = true;
    // What Fastify does to a JSON request (400 on an empty one), or else
    // what express.json() does (read it whole onto `req.body`), before Jazz.
    const server: Server = createServer((req, res) => {
      if (req.headers["content-type"] === "application/json") {
        const chunks: Buffer[] = [];
        req.on("data", (chunk: Buffer) => chunks.push(chunk));
        req.on("end", () => {
          const raw = Buffer.concat(chunks);
          parsed.push(req.url ?? "");
          if (raw.length === 0 && strict) {
            res.statusCode = 400;
            return void res.end("FST_ERR_CTP_EMPTY_JSON_BODY");
          }
          (req as { body?: unknown }).body = raw.length ? JSON.parse(`${raw}`) : {};
          jazz.nodeHandler(req, res);
        });
        return;
      }
      jazz.nodeHandler(req, res);
    });
    await jazz.attach(server);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    cleanups.push(
      () =>
        new Promise((resolve) => {
          server.closeAllConnections();
          server.close(resolve);
        }),
    );
    const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    // Sign-in asks the backend with empty POSTs: no JSON body, no JSON type.
    const alice = await userSession(jazz.appId, origin, "alice");
    await alice.db
      .insert(app.todos, { title: "signed in", owner: alice.session!.user! })
      .wait({ tier: "global" });
    expect(parsed).toEqual([]);
    // A parser that read an empty JSON body whole (an older client sends
    // one) leaves Jazz nothing to miss.
    strict = false;
    const read = await new Promise<number | undefined>((resolve, reject) => {
      const req = httpRequest(
        `${origin}/jazz/apps/${jazz.appId}/accounts/backend-user`,
        {
          method: "POST",
          headers: { "content-type": "application/json", "x-test-user": "alice" },
        },
        (res) => {
          res.resume();
          resolve(res.statusCode);
        },
      );
      req.on("error", reject);
      req.end();
    });
    expect(read).toBe(200);
    expect(parsed).toHaveLength(1);
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
    // The token names the user; they have no account until one is made.
    await expect(jazz.accountFor(await bearer("user_01"))).resolves.toBeNull();
    const account = await jazz.createAccountFor("user_01");
    expect(account).toEqual(expect.any(String));
    // A fresh token for the same user opens the same account.
    await expect(jazz.accountFor(await bearer("user_01"))).resolves.toBe(account);
    await expect(jazz.accountFor(await bearer("user_02"))).resolves.toBeNull();
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

  it("renews an open session in place on a new request before its expiresAt, unseen by the client", async () => {
    // Each answer holds for 6 s. The browser renews over HTTP before then,
    // and the socket stays open: no reconnect. Every call must be for a new
    // request: nothing replays one.
    const answered = new WeakSet<Request>();
    let calls = 0;
    const renewals: string[] = [];
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: (request) => {
        if (answered.has(request)) throw new Error("authenticate called again for one request");
        answered.add(request);
        calls += 1;
        const userId = request.headers.get("x-test-user");
        return userId ? { userId, expiresAt: inSeconds(6) } : null;
      },
      refresh: async (request, next) => {
        const renewing = request.headers.get("x-jazz-renew");
        if (renewing) renewals.push(renewing);
        const response = await next(request);
        response.headers.append("Set-Cookie", "session=rotated; Path=/; HttpOnly");
        return response;
      },
    }).ready();
    cleanups.push(() => jazz.close());
    let upgrades = 0;
    const origin = await serve(jazz, { sockets: true, onUpgrade: () => upgrades++ });
    const alice = await userSession(jazz.appId, origin, "alice");
    const owner = alice.session!.user!;
    await alice.db.insert(app.todos, { title: "before", owner }).wait({ tier: "global" });
    const opened = upgrades;
    const callsBefore = calls;

    // Past two expiries, the app sees a connected link and nothing else, on
    // the socket it opened.
    const statuses = await watchWhile(
      () => alice.db.getSyncStatus().link,
      new Promise((resolve) => setTimeout(resolve, 13_000)),
    );
    expect(renewals.length).toBeGreaterThanOrEqual(2);
    expect(new Set(renewals).size).toBe(1);
    expect(calls).toBeGreaterThan(callsBefore);
    expect(upgrades).toBe(opened);
    await alice.db.insert(app.todos, { title: "after", owner }).wait({ tier: "global" });
    const titles = (await alice.db.all(app.todos, { tier: "remote" })).map((todo) => todo.title);
    expect(titles.sort()).toEqual(["after", "before"]);
    expect(statuses).toEqual([{ state: "connected" }]);
  }, 30_000);

  it("renews a session on the HTTP stream in place too", async () => {
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: (request) => {
        const userId = request.headers.get("x-test-user");
        return userId ? { userId, expiresAt: inSeconds(6) } : null;
      },
    }).ready();
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: false });
    const previous = (globalThis as { location?: unknown }).location;
    (globalThis as { location?: unknown }).location = {
      host: new URL(origin).host,
      href: `${origin}/`,
    };
    cleanups.push(async () => {
      (globalThis as { location?: unknown }).location = previous;
    });
    const alice = await userSession(jazz.appId, origin, "alice");
    const owner = alice.session!.user!;
    await alice.db.insert(app.todos, { title: "before", owner }).wait({ tier: "global" });
    expect(requestPaths.some((path) => path.endsWith(`/apps/${jazz.appId}/stream`))).toBe(true);
    const opens = () =>
      requestPaths.filter((path) => path.endsWith(`/apps/${jazz.appId}/stream`)).length;
    const opened = opens();
    const renewals = () => requestPaths.filter((path) => path.endsWith("/backend-user")).length;
    const asked = renewals();
    await new Promise((resolve) => setTimeout(resolve, 8_000));
    expect(renewals()).toBeGreaterThan(asked);
    expect(opens()).toBe(opened);
    await alice.db.insert(app.todos, { title: "after", owner }).wait({ tier: "global" });
    expect(alice.db.getSyncStatus().link).toEqual({ state: "connected" });
  }, 30_000);

  it("re-authenticates on a new connection at its expiresAt when the browser can't renew", async () => {
    let calls = 0;
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: (request) => {
        calls += 1;
        const userId = request.headers.get("x-test-user");
        return userId ? { userId, expiresAt: inSeconds(6) } : null;
      },
      // The app's refresh fails for renewals (its auth service is down, say).
      refresh: (request, next) =>
        request.headers.get("x-jazz-renew")
          ? new Response("unavailable", { status: 503 })
          : next(request),
    }).ready();
    cleanups.push(() => jazz.close());
    let upgrades = 0;
    const origin = await serve(jazz, { sockets: true, onUpgrade: () => upgrades++ });
    const alice = await userSession(jazz.appId, origin, "alice");
    const owner = alice.session!.user!;
    await alice.db.insert(app.todos, { title: "before", owner }).wait({ tier: "global" });
    const opened = upgrades;
    const callsBefore = calls;

    const statuses = await watchWhile(
      () => alice.db.getSyncStatus().link,
      expect
        .poll(() => upgrades > opened && calls > callsBefore, { timeout: 15_000, interval: 20 })
        .toBe(true),
    );
    await alice.db.insert(app.todos, { title: "after", owner }).wait({ tier: "global" });
    const titles = (await alice.db.all(app.todos, { tier: "remote" })).map((todo) => todo.title);
    expect(titles.sort()).toEqual(["after", "before"]);
    expect(statuses).toEqual([{ state: "connected" }]);
  }, 30_000);

  it("never sends renewal frames to an older client, which reconnects at expiry as before", async () => {
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: (request) => {
        const userId = request.headers.get("x-test-user");
        return userId ? { userId, expiresAt: inSeconds(6) } : null;
      },
    }).ready();
    cleanups.push(() => jazz.close());
    let upgrades = 0;
    const origin = await serve(jazz, { sockets: true, onUpgrade: () => upgrades++ });
    // An older client: its prelude doesn't ask for renewal, and any frame it
    // doesn't know would reach its core as a protocol error.
    const RealWebSocket = globalThis.WebSocket;
    let unknownFrames = 0;
    globalThis.WebSocket = class extends RealWebSocket {
      constructor(url: string | URL, options?: unknown) {
        super(url, options as never);
        this.addEventListener("message", (event) => {
          if (!(event.data instanceof ArrayBuffer)) return;
          for (const frame of decodeWebSocketFrameBatch(new Uint8Array(event.data)))
            if (frame[0] === 0x40) unknownFrames += 1;
        });
      }
      override send(data: Parameters<WebSocket["send"]>[0]) {
        if (typeof data === "string" && data.includes('"jazz_renewal"')) {
          const prelude = JSON.parse(data) as Record<string, unknown>;
          delete prelude.jazz_renewal;
          return super.send(JSON.stringify(prelude));
        }
        return super.send(data);
      }
    } as typeof WebSocket;
    cleanups.push(async () => {
      globalThis.WebSocket = RealWebSocket;
    });
    const alice = await userSession(jazz.appId, origin, "alice");
    const owner = alice.session!.user!;
    await alice.db.insert(app.todos, { title: "before", owner }).wait({ tier: "global" });
    const opened = upgrades;
    await expect.poll(() => upgrades, { timeout: 15_000, interval: 50 }).toBeGreaterThan(opened);
    await alice.db.insert(app.todos, { title: "after", owner }).wait({ tier: "global" });
    expect(unknownFrames).toBe(0);
  }, 30_000);

  it("runs refresh only for the browser's own POST, never for a GET or another site", async () => {
    let refreshed = 0;
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate,
      refresh: (request, next) => {
        refreshed += 1;
        return next(request);
      },
    }).ready();
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: false });
    const url = `${origin}/jazz/apps/${jazz.appId}/accounts/backend-user`;
    // A cross-site navigation carries a Lax cookie on GET.
    expect(await statusOf(url, "GET", { "x-test-user": "alice" })).toBe(405);
    expect(
      await statusOf(url, "POST", { "x-test-user": "alice", origin: "https://evil.example" }),
    ).toBe(403);
    expect(refreshed).toBe(0);
    expect(await statusOf(url, "POST", { "x-test-user": "alice", origin })).toBe(200);
    expect(refreshed).toBe(1);
  }, 30_000);

  it("refuses a ticket another instance issued, falling back to reconnecting", async () => {
    const shortAuth = (request: Request) => {
      const userId = request.headers.get("x-test-user");
      return userId ? { userId, expiresAt: inSeconds(6) } : null;
    };
    // Two backends without a shared secret (each in-process core has its own).
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: shortAuth,
    }).ready();
    cleanups.push(() => jazz.close());
    const other = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: shortAuth,
    }).ready();
    cleanups.push(() => other.close());
    let upgrades = 0;
    const origin = await serve(jazz, { sockets: true, onUpgrade: () => upgrades++ });
    const otherServer = createServer((req, res) => other.nodeHandler(req, res, () => res.end()));
    await new Promise<void>((resolve) => otherServer.listen(0, "127.0.0.1", resolve));
    cleanups.push(() => new Promise((resolve) => otherServer.close(resolve)));
    const otherOrigin = `http://127.0.0.1:${(otherServer.address() as AddressInfo).port}`;
    const alice = await userSession(jazz.appId, origin, "alice");
    // Renewals land on the other instance (with this app's path).
    const viaThis = globalThis.fetch;
    let renewals = 0;
    globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
      const request = new Request(input, init);
      if (!request.headers.has("x-jazz-renew")) return viaThis(request);
      renewals += 1;
      const path = new URL(request.url).pathname.replace(jazz.appId, other.appId);
      return viaThis(new Request(`${otherOrigin}${path}`, request));
    }) as typeof fetch;
    cleanups.push(async () => {
      globalThis.fetch = viaThis;
    });
    const owner = alice.session!.user!;
    await alice.db.insert(app.todos, { title: "before", owner }).wait({ tier: "global" });
    const opened = upgrades;
    await expect.poll(() => upgrades, { timeout: 15_000, interval: 50 }).toBeGreaterThan(opened);
    expect(renewals).toBeGreaterThan(0);
    await alice.db.insert(app.todos, { title: "after", owner }).wait({ tier: "global" });
  }, 30_000);

  it("reconnects when the renewal names another user, never extending the session", async () => {
    let renewalUser = "alice";
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: (request) => {
        const userId = request.headers.has("x-jazz-renew")
          ? renewalUser
          : request.headers.get("x-test-user");
        return userId ? { userId, expiresAt: inSeconds(6) } : null;
      },
    }).ready();
    cleanups.push(() => jazz.close());
    let upgrades = 0;
    const origin = await serve(jazz, { sockets: true, onUpgrade: () => upgrades++ });
    const alice = await userSession(jazz.appId, origin, "alice");
    await alice.db
      .insert(app.todos, { title: "before", owner: alice.session!.user! })
      .wait({ tier: "global" });
    // The app's session turned into bob's (signed in as him elsewhere).
    renewalUser = "bob";
    const opened = upgrades;
    await expect.poll(() => upgrades, { timeout: 15_000, interval: 50 }).toBeGreaterThan(opened);
  }, 30_000);

  it("issues renewal tickets only for the session and user they name", async () => {
    const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: false });
    const ask = (headers: Record<string, string>) =>
      fetch(`${origin}/jazz/apps/${jazz.appId}/accounts/backend-user`, {
        method: "POST",
        headers,
      }).then(async (response) => ({
        status: response.status,
        body: response.ok ? ((await response.json()) as { renewal?: string }) : undefined,
      }));
    // An ordinary check gets no ticket; a renewal one does; signed out, none.
    expect((await ask({ "x-test-user": "alice" })).body?.renewal).toBeUndefined();
    const renewed = await ask({ "x-test-user": "alice", "x-jazz-renew": "ses-1" });
    const [payload] = renewed.body!.renewal!.split(".");
    expect(JSON.parse(Buffer.from(payload!, "base64url").toString())).toMatchObject({
      sub: "alice",
      ses: "ses-1",
    });
    // Signed out is an answer (200, no identity), and carries no ticket.
    const signedOut = await ask({ "x-jazz-renew": "ses-1" });
    expect(signedOut.body?.renewal).toBeUndefined();
  }, 30_000);

  it("keeps a session that expires in 30 days open until its sessionTtlSeconds cap", async () => {
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: (request) => {
        const userId = request.headers.get("x-test-user");
        return userId ? { userId, expiresAt: inSeconds(30 * 24 * 3600) } : null;
      },
    }).ready();
    cleanups.push(() => jazz.close());
    let upgrades = 0;
    const origin = await serve(jazz, { sockets: true, onUpgrade: () => upgrades++ });
    const alice = await userSession(jazz.appId, origin, "alice");
    await alice.db
      .insert(app.todos, { title: "long session", owner: alice.session!.user! })
      .wait({ tier: "global" });
    // Capped at sessionTtlSeconds (an hour), not re-authenticated at once.
    const opened = upgrades;
    await new Promise((resolve) => setTimeout(resolve, 2_000));
    expect(upgrades).toBe(opened);
    expect(alice.db.getSyncStatus().link).toEqual({ state: "connected" });
  }, 30_000);

  it("ends a session at its expiresAt once authenticate no longer names the user", async () => {
    const signedOut = new Set<string>();
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: (request) => {
        const userId = request.headers.get("x-test-user");
        return userId && !signedOut.has(userId) ? { userId, expiresAt: inSeconds(6) } : null;
      },
    }).ready();
    cleanups.push(() => jazz.close());
    let upgrades = 0;
    const origin = await serve(jazz, { sockets: true, onUpgrade: () => upgrades++ });
    const alice = await userSession(jazz.appId, origin, "alice");
    await alice.db
      .insert(app.todos, { title: "signed in", owner: alice.session!.user! })
      .wait({ tier: "global" });

    // She signs out elsewhere: the re-authentication is refused, as an auth
    // failure, so the client stops retrying and its session asks the backend
    // who is signed in.
    signedOut.add("alice");
    await expect
      .poll(() => alice.db.getSyncStatus().link, { timeout: 15_000, interval: 50 })
      .toMatchObject({ state: "offline", reason: "auth" });
    const refused = upgrades;
    await new Promise((resolve) => setTimeout(resolve, 1_000));
    expect(upgrades).toBe(refused);
  }, 30_000);

  it("ends a user's open sessions at once on revokeUser", async () => {
    const signedOut = new Set<string>();
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: (request) => {
        const userId = request.headers.get("x-test-user");
        return userId && !signedOut.has(userId) ? { userId, expiresAt: inAnHour() } : null;
      },
    }).ready();
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: true });
    const alice = await userSession(jazz.appId, origin, "alice");
    await alice.db
      .insert(app.todos, { title: "signed in", owner: alice.session!.user! })
      .wait({ tier: "global" });

    signedOut.add("alice");
    expect(await jazz.revokeUser("bob")).toBe(0);
    expect(await jazz.revokeUser("alice")).toBe(1);
    await expect
      .poll(() => alice.db.getSyncStatus().link, { timeout: 5_000, interval: 20 })
      .toMatchObject({ state: "offline", reason: "auth" });
    await expect(jazz.revokeUser("")).rejects.toThrow(/pass the user's id/);
  }, 30_000);

  it("picks up a user's changed claims when the session re-authenticates", async () => {
    // Anyone reads their own todos; only an admin (a claim) may create one.
    const byRole = s.definePermissions(app, ({ policy, session }) => {
      policy.todos.allowRead.where({ owner: session.user });
      policy.todos.allowInsert.where(session.where({ "claims.role": "admin" }));
      policy.todos.allowUpdate.never();
      policy.todos.allowDelete.never();
    });
    const roles = new Map([["alice", "admin"]]);
    const jazz = await createJazz({
      app: await appWith(byRole),
      permissions: byRole,
      store: memory(),
      authenticate: (request) => {
        const userId = request.headers.get("x-test-user");
        return userId
          ? { userId, claims: { role: roles.get(userId) ?? "none" }, expiresAt: inSeconds(6) }
          : null;
      },
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
    await session.refresh();
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

    // The app demotes her: her next session carries the claims authenticate
    // names now.
    roles.set("alice", "viewer");
    const opened = upgrades;
    await expect.poll(() => upgrades, { timeout: 15_000, interval: 50 }).toBeGreaterThan(opened);
    await expect
      .poll(() => alice.db.getSyncStatus().link, { timeout: 10_000, interval: 50 })
      .toEqual({ state: "connected" });
    expect(await outcome(alice.db.insert(app.todos, { title: "after demotion", owner }))).toBe(
      "permission_denied",
    );
  }, 60_000);

  it("refuses an authenticate answer without a valid expiresAt, explicitly", async () => {
    let answer: unknown = { userId: "alice" };
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: () => answer as never,
    }).ready();
    cleanups.push(() => jazz.close());
    const request = () => new Request("http://app.test/");
    await expect(jazz.forReqUser(request())).rejects.toThrow(
      /authenticate must return expiresAt with the user.*It returned none/,
    );
    answer = { userId: "alice", expiresAt: Date.now() + 60_000 };
    await expect(jazz.forReqUser(request())).rejects.toThrow(/looks like milliseconds/);
    answer = { userId: "alice", expiresAt: Number.NaN };
    await expect(jazz.forReqUser(request())).rejects.toThrow(/It returned NaN/);
    // A valid answer passes the check: alice just has no account yet.
    answer = { userId: "alice", expiresAt: inAnHour() };
    await expect(jazz.forReqUser(request())).rejects.toThrow(/has no Jazz account yet/);
  });

  it("answers a signed-out backend-user probe with 200 and no user, and every client reads it as signed out (#5171)", async () => {
    const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: false });
    const registry = `${origin}/jazz/apps/${jazz.appId}/accounts`;

    // Before: 401 signed_out, a red line in devtools on every signed-out
    // page load. Now: an answer saying no one is signed in.
    const probe = await fetch(`${registry}/backend-user`, { method: "POST" });
    expect(probe.status).toBe(200);
    expect(probe.headers.get("cache-control")).toBe("no-store");
    const body = (await probe.json()) as { identity?: unknown };
    expect(body).toEqual({ user: null });

    // An alpha.60/61 client (`if (!answer.identity) return null`) reads it
    // as signed out, as it read the 401.
    expect(body.identity).toBeFalsy();
    // So does this one.
    await expect(signedInIdentity(registry)).resolves.toBeNull();

    // The routes that do something keep refusing a signed-out request.
    for (const route of ["backend-sign-in", "backend-link", "backend-link/challenge"]) {
      const refused = await fetch(`${registry}/${route}`, { method: "POST" });
      expect({ route, status: refused.status }).toEqual({ route, status: 401 });
      expect(await refused.text()).toBe("signed_out");
    }

    // A backend still on alpha.61 or earlier answers 401: still signed out.
    const oldBackend = (async () =>
      new Response("signed_out", { status: 401 })) as unknown as typeof fetch;
    await expect(signedInIdentity(registry, oldBackend)).resolves.toBeNull();
    // And a signed-in answer still names the user.
    const signedIn = (async (input: RequestInfo | URL, init?: RequestInit) =>
      fetch(input, {
        ...init,
        headers: { "x-test-user": "alice" },
      })) as typeof fetch;
    await expect(signedInIdentity(registry, signedIn)).resolves.toMatchObject({
      issuer: "jazz:backend",
      subject: "alice",
      hasAccount: false,
    });
  });

  it("answers 500 naming authenticate when it throws, without an unhandled rejection (#5263)", async () => {
    let fail: () => never | Promise<never> = () => {
      throw new Error("synthetic auth failure");
    };
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: () => fail(),
    }).ready();
    cleanups.push(() => jazz.close());
    const errors = vi.spyOn(console, "error").mockImplementation(() => undefined);
    cleanups.push(async () => errors.mockRestore());
    const unhandled = vi.fn();
    process.on("unhandledRejection", unhandled);
    cleanups.push(async () => void process.off("unhandledRejection", unhandled));
    const origin = await serve(jazz, { sockets: false });
    const url = `http://app.test/jazz/apps/${jazz.appId}/accounts/backend-user`;

    // Before: `next()`'s POST rejected with the app's error, which on Vercel
    // surfaced as an unhandledRejection and the function exited.
    const { POST } = jazz.next();
    for (const thrower of [
      () => {
        throw new Error("synthetic auth failure");
      },
      () => Promise.reject(new Error("synthetic auth failure")),
    ]) {
      fail = thrower;
      errors.mockClear();
      for (const answer of [
        await POST(new Request(url, { method: "POST" })),
        await jazz.handle(new Request(url, { method: "POST" })),
        await fetch(`${origin}/jazz/apps/${jazz.appId}/accounts/backend-sign-in`, {
          method: "POST",
        }),
      ]) {
        expect(answer.status).toBe(500);
        expect(answer.headers.get("cache-control")).toBe("no-store");
        // Only the code: nothing of the app's error.
        expect(await answer.text()).toBe("authenticate_failed");
      }
      // Logged once per request, naming authenticate, with the app's error.
      expect(errors).toHaveBeenCalledTimes(3);
      for (const [message, error] of errors.mock.calls) {
        expect(message).toMatch(/^createJazz: your authenticate threw/);
        expect((error as Error).message).toBe("synthetic auth failure");
      }
    }
    // The client reports the stable code.
    await expect(
      requestBackendUser(`${origin}/jazz/apps/${jazz.appId}/accounts`),
    ).rejects.toMatchObject({ code: "authenticate_failed", status: 500 });
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(unhandled).not.toHaveBeenCalled();
  });

  // The identity provider being down is not the app's bug: sign-in answers
  // 503 with Retry-After (the client retries and stays signed in), and the
  // log names the JWKS rather than telling the developer to fix their code.
  it("answers 503 with Retry-After when fromJWT's JWKS is down, without blaming authenticate (#5367)", async () => {
    const jwks: Server = createServer((_req, res) => {
      res.statusCode = 503;
      res.end("synthetic outage");
    });
    await new Promise<void>((resolve) => jwks.listen(0, "127.0.0.1", resolve));
    cleanups.push(() => new Promise((resolve) => jwks.close(resolve)));
    const jwksUrl = `http://127.0.0.1:${(jwks.address() as AddressInfo).port}/jwks.json`;
    const { privateKey } = await generateKeyPair("ES256");
    const issuer = "https://idp.example";
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: fromJWT({ jwksUrl, issuer, audience: "my-app" }),
    }).ready();
    cleanups.push(() => jazz.close());
    const errors = vi.spyOn(console, "error").mockImplementation(() => undefined);
    cleanups.push(async () => errors.mockRestore());
    const origin = await serve(jazz, { sockets: false });
    const token = await new SignJWT({})
      .setProtectedHeader({ alg: "ES256", kid: "k1" })
      .setIssuer(issuer)
      .setAudience("my-app")
      .setSubject("alice")
      .setExpirationTime("5m")
      .sign(privateKey);
    const headers = { authorization: `Bearer ${token}` };
    const path = `/jazz/apps/${jazz.appId}/accounts`;
    const { POST } = jazz.next();
    for (const answer of [
      await POST(new Request(`http://app.test${path}/backend-user`, { method: "POST", headers })),
      await jazz.handle(
        new Request(`http://app.test${path}/backend-user`, { method: "POST", headers }),
      ),
      await fetch(`${origin}${path}/backend-sign-in`, { method: "POST", headers }),
    ]) {
      expect(answer.status).toBe(503);
      expect(answer.headers.get("retry-after")).toBe("5");
      expect(answer.headers.get("cache-control")).toBe("no-store");
      expect(await answer.text()).toBe("authentication_unavailable");
    }
    expect(errors).toHaveBeenCalledTimes(3);
    for (const [logged] of errors.mock.calls) {
      expect(String(logged)).not.toMatch(/your authenticate threw/);
      expect((logged as Error).message).toContain(`JWKS ${jwksUrl} is unavailable`);
    }
    // The client sees a retryable 503, not a refusal.
    await expect(
      requestBackendUser(`${origin}${path}`, (input, init) =>
        fetch(input, { ...init, headers: { ...init?.headers, ...headers } }),
      ),
    ).rejects.toMatchObject({ status: 503 });
  });

  // A JWKS URL that answers, but not with a JWKS, is the app's setup and not
  // an outage: retrying won't fix it, so it stays a 500 naming authenticate.
  it("answers 500 naming authenticate when fromJWT's JWKS answers with something that isn't a JWKS (#5367)", async () => {
    const jwks: Server = createServer((_req, res) => res.end("<html>not a JWKS</html>"));
    await new Promise<void>((resolve) => jwks.listen(0, "127.0.0.1", resolve));
    cleanups.push(() => new Promise((resolve) => jwks.close(resolve)));
    const jwksUrl = `http://127.0.0.1:${(jwks.address() as AddressInfo).port}/jwks.json`;
    const { privateKey } = await generateKeyPair("ES256");
    const issuer = "https://idp.example";
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: fromJWT({ jwksUrl, issuer, audience: "my-app" }),
    }).ready();
    cleanups.push(() => jazz.close());
    const errors = vi.spyOn(console, "error").mockImplementation(() => undefined);
    cleanups.push(async () => errors.mockRestore());
    const token = await new SignJWT({})
      .setProtectedHeader({ alg: "ES256", kid: "k1" })
      .setIssuer(issuer)
      .setAudience("my-app")
      .setSubject("alice")
      .setExpirationTime("5m")
      .sign(privateKey);
    const answer = await jazz.handle(
      new Request(`http://app.test/jazz/apps/${jazz.appId}/accounts/backend-user`, {
        method: "POST",
        headers: { authorization: `Bearer ${token}` },
      }),
    );
    expect(answer.status).toBe(500);
    expect(await answer.text()).toBe("authenticate_failed");
    expect(errors).toHaveBeenCalledTimes(1);
    const [message, error] = errors.mock.calls[0]!;
    expect(message).toMatch(/^createJazz: your authenticate threw/);
    expect((error as Error).message).toBe("Unable to parse JWKS response");
  });

  it("tells the browser by a stable code that authenticate returned no expiresAt, and the client names the fix", async () => {
    let answer: unknown = { userId: "alice", internal: "secret-session-detail" };
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: () => answer as never,
    }).ready();
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: false });
    const errors = vi.spyOn(console, "error").mockImplementation(() => undefined);
    cleanups.push(async () => errors.mockRestore());
    const registry = `${origin}/jazz/apps/${jazz.appId}/accounts`;

    // Before: a bare 500 the browser reported as "Jazz could not ask your
    // backend who is signed in", the reason only in the server's log.
    const response = await fetch(`${registry}/backend-user`, { method: "POST" });
    expect(response.status).toBe(500);
    // Only the code: nothing of what the app's authenticate returned.
    expect(await response.text()).toBe("authenticate_expires_at_missing");
    // The server logs the detail, once.
    expect(errors).toHaveBeenCalledWith(
      expect.stringMatching(/authenticate must return expiresAt with the user.*It returned none/),
    );

    // The client's error (the session's `refresh()` and sign-in reject with
    // it, or with it as their cause) names the fix.
    for (const ask of [() => requestBackendUser(registry), () => requestBackendSignIn(registry)]) {
      const error = await ask().then(
        () => undefined,
        (error: unknown) => error as { code?: string; status?: number; message?: string },
      );
      expect(error).toMatchObject({ code: "authenticate_expires_at_missing", status: 500 });
      expect(error?.message).toMatch(
        /^authenticate_expires_at_missing: your backend's authenticate \(createJazz\) named a user without a valid expiresAt \(none, not a number, or in milliseconds\).*Return expiresAt with the user/,
      );
    }
    // An expiresAt in milliseconds (or not a number) is the same mistake, by the same code.
    for (const expiresAt of [Date.now() + 60_000, "tomorrow"]) {
      answer = { userId: "alice", expiresAt };
      const wrong = await fetch(`${registry}/backend-user`, { method: "POST" });
      expect(wrong.status).toBe(500);
      expect(await wrong.text()).toBe("authenticate_expires_at_missing");
    }
    // And it says so on the console, once, for the session's background checks.
    expect(
      errors.mock.calls.filter(([line]) => String(line).startsWith("Jazz: authenticate_expires")),
    ).toHaveLength(1);
  });

  it("refuses a sync session whose authenticate answer has already expired", async () => {
    let expired = false;
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: (request) => {
        const userId = request.headers.get("x-test-user");
        return userId ? { userId, expiresAt: expired ? inSeconds(-60) : inAnHour() } : null;
      },
    }).ready();
    cleanups.push(() => jazz.close());
    const error = vi.spyOn(console, "error").mockImplementation(() => {});
    cleanups.push(async () => error.mockRestore());
    const origin = await serve(jazz, { sockets: true });
    const alice = await userSession(jazz.appId, origin, "alice");
    await alice.db
      .insert(app.todos, { title: "signed in", owner: alice.session!.user! })
      .wait({ tier: "global" });
    expired = true;
    expect(await jazz.revokeUser("alice")).toBe(1);
    await expect
      .poll(() => alice.db.getSyncStatus().link, { timeout: 5_000, interval: 20 })
      .toMatchObject({ state: "offline", reason: "auth" });
    // A reconnect is refused too, and the cause is logged once for the app.
    await alice.db.reconnect().catch(() => undefined);
    await expect
      .poll(
        () =>
          error.mock.calls.filter(([message]) =>
            String(message).includes("authenticate named a user with an expiresAt in the past"),
          ).length,
        { timeout: 5_000 },
      )
      .toBe(1);
  }, 30_000);

  it("refuses recheckSeconds, which was removed, with a pointer to expiresAt", () => {
    expect(() =>
      createJazz({
        app,
        permissions,
        store: memory(),
        authenticate,
        recheckSeconds: 60,
      } as never),
    ).toThrow(/recheckSeconds was removed.*expiresAt.*revokeUser/);
  });

  it("answers the client's socket probe as it answers the socket, origin check included", async () => {
    const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: true });
    const probe = (headers: Record<string, string>) =>
      fetch(`${origin}/jazz/apps/${jazz.appId}/ws`, { method: "POST", headers });
    // This host upgrades: retry the socket.
    const same = await probe({ origin });
    expect(same.status).toBe(426);
    expect(same.headers.get("x-jazz-stream")).toBeNull();
    // A page whose origin the socket is refused for hears so.
    expect((await probe({ origin: "http://elsewhere.test" })).status).toBe(403);
  });

  it("refuses local-first guests unless authenticate accepts them", async () => {
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: (request) => {
        const userId = request.headers.get("x-test-user");
        return userId ? { userId, expiresAt: inAnHour() } : null;
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
      initial: "device",
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

  // A guest whose browser just signed up (the app set the user's cookie)
  // reconnects before it links: its writes still upload as the guest's.
  it("lets a guest sync while a user's cookie rides on its socket", async () => {
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      // The documented setup: the session cookie's user, else a guest.
      authenticate: (request) => {
        const user = /(?:^|; )sid=([^;]+)/.exec(request.headers.get("cookie") ?? "")?.[1];
        return user ? { userId: user, expiresAt: inAnHour() } : acceptDeviceAccounts(request);
      },
    });
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: true });
    const RealWebSocket = globalThis.WebSocket;
    globalThis.WebSocket = class extends RealWebSocket {
      constructor(url: string | URL, options?: { headers?: Record<string, string> }) {
        const cookie = new URL(url).host === new URL(origin).host ? { cookie: "sid=alice" } : {};
        super(url, { ...options, headers: { ...options?.headers, ...cookie } } as never);
      }
    } as typeof WebSocket;
    cleanups.push(async () => {
      globalThis.WebSocket = RealWebSocket;
    });
    const guestSession = await createJazzClient({
      app,
      permissions,
      serverUrl: `${origin}/jazz`,
      driver: { type: "memory" },
      initial: "device",
    });
    cleanups.push(() => guestSession.close());
    const guest = guestSession.getSnapshot().client!;
    const row = await guest.db
      .insert(app.todos, { title: "the guest's", owner: guest.session!.user! })
      .wait({ tier: "global" });
    expect((await jazz.db.all(app.todos.where({ id: row.id }))).map((t) => t.title)).toEqual([
      "the guest's",
    ]);
  }, 30_000);

  it("lets a guest in anew each sessionTtlSeconds, and not once guests are off", async () => {
    let guestsWelcome = true;
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: (request) => (guestsWelcome ? authenticate(request) : null),
      sessionTtlSeconds: 1,
    });
    cleanups.push(() => jazz.close());
    let upgrades = 0;
    const origin = await serve(jazz, { sockets: true, onUpgrade: () => upgrades++ });
    const guestSession = await createJazzClient({
      app,
      permissions,
      serverUrl: `${origin}/jazz`,
      driver: { type: "memory" },
      initial: "device",
    });
    cleanups.push(() => guestSession.close());
    const guest = guestSession.getSnapshot().client!;
    await guest.db
      .insert(app.todos, { title: "synced", owner: guest.session!.user! })
      .wait({ tier: "global" });
    const opened = upgrades;
    const statuses = await watchWhile(
      () => guest.db.getSyncStatus().link,
      expect.poll(() => upgrades, { timeout: 10_000, interval: 20 }).toBeGreaterThan(opened),
    );
    expect(statuses).toEqual([{ state: "connected" }]);

    guestsWelcome = false;
    await expect
      .poll(() => guest.db.getSyncStatus().link, { timeout: 10_000, interval: 50 })
      .toMatchObject({ state: "offline", reason: "auth" });
  }, 30_000);

  // A bundler may evaluate jazz-tools/backend more than once (Next dev, per
  // route): createJazz then returns the Jazz the first copy opened for the
  // store, with the later copy's options, so its guest marker must count.
  it("lets guests in through acceptDeviceAccounts from another copy of the module", async () => {
    const dir = await mkdtemp(join(tmpdir(), "jazz-two-copies-"));
    cleanups.push(() => rm(dir, { recursive: true, force: true }));
    const first = createJazz({ app, permissions, store: sqlite({ path: dir }), authenticate });
    cleanups.push(() => first.close());

    vi.resetModules();
    const copy = (await import("./index.js")) as typeof import("./index.js");
    expect(copy.acceptDeviceAccounts).not.toBe(acceptDeviceAccounts);
    expect(copy.acceptDeviceAccounts(new Request("http://app/"))).not.toBe(
      acceptDeviceAccounts(new Request("http://app/")),
    );
    const jazz = copy.createJazz({
      app,
      permissions,
      store: copy.sqlite({ path: dir }),
      authenticate: (request) => {
        const userId = request.headers.get("x-test-user");
        return userId ? { userId, expiresAt: inAnHour() } : copy.acceptDeviceAccounts(request);
      },
    });
    expect(jazz).toBe(first);
    await jazz.ready();
    const origin = await serve(jazz, { sockets: true });

    // A guest is no one signed in, not a user without an id.
    const backendUser = await fetch(`${origin}/jazz/apps/${jazz.appId}/accounts/backend-user`, {
      method: "POST",
    });
    expect(await backendUser.json()).toEqual({ user: null });
    await expect(jazz.accountFor(new Request("http://app/x"))).resolves.toBeNull();

    // And its sync session is let in.
    const guestSession = await createJazzClient({
      app,
      permissions,
      serverUrl: `${origin}/jazz`,
      driver: { type: "memory" },
      initial: "device",
    });
    cleanups.push(() => guestSession.close());
    const guest = guestSession.getSnapshot().client!;
    await guest.db
      .insert(app.todos, { title: "synced", owner: guest.session!.user! })
      .wait({ tier: "global" });
    expect((await jazz.db.all(app.todos)).map((todo) => todo.title)).toEqual(["synced"]);
  }, 60_000);

  it("names an authenticate answer without a userId as the app's bug, before asking the core", async () => {
    let answer: unknown;
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: () => answer as never,
    }).ready();
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: false });
    const errors = vi.spyOn(console, "error").mockImplementation(() => undefined);
    cleanups.push(async () => errors.mockRestore());
    const request = () => new Request("http://app/x");
    for (const [wrong, shown] of [
      [{}, /returned an object with keys \[\]$/],
      [{ id: "alice" }, /returned an object with keys \[id\]$/],
      [{ userId: 42 }, /keys \[userId\], userId: a number$/],
      [{ userId: "" }, /keys \[userId\], userId: an empty string$/],
      [{ userId: { token: "secret-token" } }, /keys \[userId\], userId: an object$/],
      ["secret-token", /returned a string$/],
    ] as const) {
      answer = wrong;
      await expect(jazz.accountFor(request())).rejects.toThrow(
        /^createJazz: authenticate must return \{ userId: string, expiresAt: number \}, acceptDeviceAccounts\(request\) or null/,
      );
      await expect(jazz.accountFor(request())).rejects.toThrow(shown);
      // It names the answer's shape, never its values: a wrong answer may be a token.
      const error = await jazz.accountFor(request()).catch((caught: Error) => caught);
      expect(String(error)).not.toMatch(/secret-token/);
      const response = await fetch(`${origin}/jazz/apps/${jazz.appId}/accounts/backend-user`, {
        method: "POST",
      });
      expect(response.status).toBe(500);
      expect(await response.text()).not.toMatch(/422|subject/);
    }
    // A user and no one still work as before.
    answer = { userId: "alice", expiresAt: inAnHour() };
    await expect(jazz.accountFor(request())).resolves.toBeNull();
    answer = undefined;
    await expect(jazz.accountFor(request())).resolves.toBeNull();
  }, 30_000);

  // An Expo app is a CommonJS package: an ESM backend importing its
  // permissions.ts by default gets the whole module.exports, `{ default }`.
  it("names a permissions module imported as CommonJS, not an unknown table `default`", async () => {
    const dir = await mkdtemp(join(tmpdir(), "jazz-cjs-permissions-"));
    cleanups.push(() => rm(dir, { recursive: true, force: true }));
    const key = "jazz-tools.test.create-jazz.cjs-permissions";
    (globalThis as Record<symbol, unknown>)[Symbol.for(key)] = permissions;
    const file = join(dir, "permissions.js");
    await writeFile(
      file,
      `"use strict";\nObject.defineProperty(exports, "__esModule", { value: true });\nexports.default = globalThis[Symbol.for(${JSON.stringify(key)})];\n`,
    );
    const wrapped = createRequire(file)(file) as typeof permissions;
    const jazz = createJazz({ app, permissions: wrapped, store: memory() });
    cleanups.push(() => jazz.close());
    await expect(jazz.ready()).rejects.toThrow(
      /^permissions\.ts arrived as \{ default: … \}.*\{"type": "module"\}/,
    );
  }, 30_000);

  it("refuses a sync session for a user without an account, until they sign in", async () => {
    // One origin whose backend is replaced by one on a new core (a reset dev
    // store): the client remembers alice, but the new core has no account
    // for her, and sync admission never creates one. (A browser's session
    // then signs her in again: see backend-watch.test.ts.)
    const listen = async (jazz: Jazz, port = 0) => {
      const server = createServer((req, res) => jazz.nodeHandler(req, res));
      // Upgraded sync sockets too, which `server.close` would wait on.
      const sockets = new Set<Socket>();
      server.on("connection", (socket) => {
        sockets.add(socket);
        socket.once("close", () => sockets.delete(socket));
      });
      await jazz.attach(server);
      await new Promise<void>((resolve) => server.listen(port, "127.0.0.1", resolve));
      const close = () =>
        new Promise<void>((resolve) => {
          for (const socket of sockets) socket.destroy();
          server.close(() => resolve());
        });
      cleanups.push(close);
      return { port: (server.address() as AddressInfo).port, close };
    };
    const first = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    cleanups.push(() => first.close());
    const before = await listen(first);
    const origin = `http://127.0.0.1:${before.port}`;
    signInAs(origin, "alice");
    const session = await createJazzClient({
      app,
      permissions,
      serverUrl: `${origin}/jazz`,
      driver: { type: "memory" },
    });
    cleanups.push(() => session.close());
    await session.refresh();
    const firstAccount = session.getSnapshot().client!.session!.user!;
    await session
      .getSnapshot()
      .client!.db.insert(app.todos, { title: "before the reset", owner: firstAccount })
      .wait({ tier: "global" });

    await before.close();
    const second = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    cleanups.push(() => second.close());
    await listen(second, before.port);

    // The client reconnects stating alice's session: the new core has no
    // account for her, and admission refuses it rather than make one.
    const remembered = session.getSnapshot().client!;
    await expect
      .poll(() => remembered.db.getSyncStatus().link, { timeout: 20_000, interval: 100 })
      .toMatchObject({ state: "offline", reason: "auth" });
    const asAlice = new Request("http://app/x", { headers: { "x-test-user": "alice" } });
    await expect(second.accountFor(asAlice)).resolves.toBeNull();

    // Her sign-in gives her an account on the new core, and syncs.
    const again = await createJazzClient({
      app,
      permissions,
      serverUrl: `${origin}/jazz`,
      driver: { type: "memory" },
    });
    cleanups.push(() => again.close());
    await again.refresh();
    const account = again.getSnapshot().client!.session!.user!;
    expect(account).not.toBe(firstAccount);
    await expect(second.accountFor(asAlice)).resolves.toBe(account);
    await again
      .getSnapshot()
      .client!.db.insert(app.todos, { title: "after the reset", owner: account })
      .wait({ tier: "global" });
  }, 60_000);

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
        initial: "device",
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
      await session.linkDeviceAccount();
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

  it("links a guest on a page that has its own window.__jazz, and leaves that global alone (#5262)", async () => {
    // Before: the session refused to start, or (rc.2) `linkDeviceAccount()`
    // rejected with "window.__jazz is reserved for Jazz"; on rc.1 a sealed
    // one failed the upgrade with "object is not extensible".
    const own = Object.seal({ mine: true });
    vi.stubGlobal("window", { document: {}, __jazz: own });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    cleanups.push(async () => {
      warn.mockRestore();
      vi.unstubAllGlobals();
    });
    const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: true });

    const session = await createJazzClient({
      appId: jazz.appId,
      app,
      permissions,
      serverUrl: `${origin}/jazz`,
      driver: { type: "memory" },
      initial: "device",
    });
    cleanups.push(() => session.close());
    expect(session.getSnapshot().status).toBe("ready");
    const account = session.getSnapshot().client!.session!.user!;

    signInAs(origin, "alice");
    await session.linkDeviceAccount();
    const alice = session.getSnapshot().client!;
    expect(alice.session!.user!).toBe(account);
    expect(alice.session!.identity.subject).toBe("alice");

    // The app's global is as it set it; Jazz said so once.
    expect((globalThis as { window?: { __jazz?: unknown } }).window!.__jazz).toBe(own);
    expect(own).toEqual({ mine: true });
    const told = warn.mock.calls.filter(([message]) => String(message).includes("window.__jazz"));
    expect(told).toHaveLength(1);
  }, 60_000);

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
    expect(await (await post("backend-user")).json()).toEqual({ user: null });
    // Asked as often as a page focuses, it never assigns an account.
    for (let i = 0; i < 2; i++) {
      const answer = (await (await post("backend-user", "alice")).json()) as {
        account: string | null;
        identity: { issuer: string; subject: string };
      };
      expect(answer.account).toBeNull();
      expect(answer.identity).toEqual({ issuer: "jazz:backend", subject: "alice" });
    }
    // Nor do the backend's own lookups and requests for her (V9): an API
    // route that asks for her account, or acts as her, before her browser's
    // guest links leaves the link open.
    const asAlice = () => new Request("http://app/x", { headers: { "x-test-user": "alice" } });
    await expect(jazz.accountFor(asAlice())).resolves.toBeNull();
    await expect(jazz.forReqUser(asAlice())).rejects.toMatchObject({ code: "no_account" });
    await expect(jazz.withAttribution(asAlice())).rejects.toMatchObject({ code: "no_account" });
    // createAccountFor needs the user's id, and rejects without one.
    await expect(jazz.createAccountFor("")).rejects.toThrow(/pass the user's id/);
    await expect(jazz.createAccountFor(undefined as unknown as string)).rejects.toThrow(
      /pass the user's id/,
    );
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
    await expect(jazz.accountFor(asAlice())).resolves.toBe(account);
    await expect(jazz.forReqUser(asAlice())).resolves.toBeDefined();
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

  it("refuses a token it can't verify as invalid, and no credential as missing (#4952)", async () => {
    const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: true });
    const refusal = async (prelude: unknown) => {
      const socket = new NodeWebSocket(
        `${origin.replace("http", "ws")}/jazz/apps/${jazz.appId}/ws`,
      );
      socket.binaryType = "arraybuffer";
      socket.addEventListener("open", () => socket.send(JSON.stringify(prelude)));
      const messages: string[] = [];
      socket.addEventListener("message", (event) =>
        messages.push(new TextDecoder().decode(new Uint8Array(event.data as ArrayBuffer))),
      );
      const closed = await new Promise<{ code: number; reason: string }>((resolve) =>
        socket.addEventListener("close", (event) =>
          resolve({ code: event.code, reason: event.reason }),
        ),
      );
      expect(closed.code).toBe(1008);
      expect(messages).toHaveLength(1);
      return messages[0]!;
    };
    // A structurally valid JWT from an issuer this backend doesn't verify
    // (forged signature): a credential was presented and failed.
    const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString("base64url");
    const forged = [
      encode({ alg: "HS256", typ: "JWT" }),
      encode({ iss: "https://issuer.example", sub: "alice", exp: inAnHour() }),
      Buffer.from("not-a-signature").toString("base64url"),
    ].join(".");
    // Before: "session missing: not signed in", reported as `missing`.
    const invalid = await refusal({ peer_identity: "forged", auth: { jwt_token: forged } });
    expect(invalid).toContain("session invalid");
    expect(mapAuthReason(invalid)).toBe("invalid");
    // Not even a JWT.
    expect(
      mapAuthReason(await refusal({ peer_identity: "junk", auth: { jwt_token: "junk" } })),
    ).toBe("invalid");

    // No credential at all (a stated session, no cookie) stays `missing`.
    const missing = await refusal({
      peer_identity: "nobody",
      backend_session: { issuer: "jazz:backend", user_id: "alice" },
    });
    expect(missing).toContain("session missing");
    expect(mapAuthReason(missing)).toBe("missing");
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

  it("lets a client end its stream session once signed out, and calls one already ended done", async () => {
    // #5187: a client ends its stream as it signs out, when the app's
    // sign-in is already gone. That was a 403, and a DELETE that arrived
    // after the session ended a 404 or 410: errors the browser logs.
    const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    const appId = jazz.appId;
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: false });
    const previous = (globalThis as { location?: unknown }).location;
    (globalThis as { location?: unknown }).location = {
      host: new URL(origin).host,
      href: `${origin}/`,
    };
    cleanups.push(async () => {
      (globalThis as { location?: unknown }).location = previous;
    });
    const alice = await userSession(appId, origin, "alice");
    await alice.db
      .insert(app.todos, { title: "over the stream", owner: alice.session!.user! })
      .wait({ tier: "global" });
    const sessionPath = requestPaths.find((path) => /\/stream\/[^/]+$/.test(path))!;
    const key = streamKeys.get(sessionPath)!;
    const end = (headers: Record<string, string>) =>
      statusOf(`${origin}${sessionPath}`, "DELETE", headers);

    // Still refused: no key, or the key with someone else's sign-in.
    expect(await end({})).toBe(403);
    expect(await end({ "x-test-user": "bob", "x-jazz-stream-key": key })).toBe(403);
    // Its own client, signed out: ends it.
    expect(await end({ "x-jazz-stream-key": key })).toBe(204);
    // Ended already: done, for its client only.
    expect(await end({ "x-jazz-stream-key": key })).toBe(204);
    expect(await end({ "x-test-user": "alice" })).toBe(403);
  }, 60_000);

  for (const sockets of [false, true]) {
    it(`uploads more than a session takes at once over ${sockets ? "a socket" : "the HTTP stream"}, on one session`, async () => {
      // 24 MB local-first: three times a session's 8 MiB ingress budget,
      // sent as fast as the client can. The core reads it at its own pace;
      // it used to drop the session, and the upload restarted forever.
      const blobs = s.defineApp(
        { files: s.table({ name: s.string(), owner: s.uuid(), data: s.bytes() }, {}) },
        { migrations: "none" },
      );
      const blobPermissions = s.definePermissions(blobs, ({ policy, session }) => {
        policy.files.allowRead.where({ owner: session.user });
        policy.files.allowInsert.where({ owner: session.user });
        policy.files.allowUpdate.never();
        policy.files.allowDelete.never();
      });
      const jazz = await createJazz({
        app: blobs,
        permissions: blobPermissions,
        store: memory(),
        authenticate,
      }).ready();
      cleanups.push(() => jazz.close());
      let upgrades = 0;
      const origin = await serve(
        jazz,
        // A host that can't upgrade leaves the upgrade unanswered.
        sockets ? { sockets, onUpgrade: () => (upgrades += 1) } : { sockets },
      );
      const previous = (globalThis as { location?: unknown }).location;
      (globalThis as { location?: unknown }).location = {
        host: new URL(origin).host,
        href: `${origin}/`,
      };
      cleanups.push(async () => {
        (globalThis as { location?: unknown }).location = previous;
      });
      signInAs(origin, "alice");
      const session = await createJazzClient({
        appId: jazz.appId,
        app: blobs,
        permissions: blobPermissions,
        serverUrl: `${origin}/jazz`,
        driver: { type: "memory" },
      });
      cleanups.push(() => session.close());
      await session.refresh();
      const alice = session.getSnapshot().client!;

      const data = new Uint8Array(24 * 1024 * 1024);
      for (let i = 0; i < data.length; i += 4096) data[i] = (i * 2654435761) >>> 24;
      const row = await alice.db
        .insert(blobs.files, { name: "big", owner: alice.session!.user!, data })
        .wait({ tier: "global" });
      const [seen] = await jazz.db.all(blobs.files.where({ id: row.id }));
      expect(seen?.data.byteLength).toBe(data.byteLength);
      expect(Buffer.from(seen!.data).equals(Buffer.from(data))).toBe(true);
      // One session carried it all: none was dropped and opened again.
      if (sockets) expect(upgrades).toBe(1);
      else
        expect(
          requestPaths.filter((path) => path.endsWith(`/apps/${jazz.appId}/stream`)),
        ).toHaveLength(1);
    }, 120_000);
  }

  it("ends the session of a socket that can't pause once its client sends far ahead of the core", async () => {
    // Sign-in never finishes, so nothing the client sends is taken.
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: () => new Promise<never>(() => {}),
      upgradesSockets: true,
    }).ready();
    cleanups.push(() => jazz.close());
    const listeners: Record<string, (...args: unknown[]) => void> = {};
    const closes: Array<[number | undefined, string | undefined]> = [];
    jazz.acceptSocket(new Request(`http://app.test/jazz/apps/${jazz.appId}/ws`), {
      send() {},
      close: (code, reason) => closes.push([code, reason]),
      on(event: string, listener: (...args: never[]) => void) {
        listeners[event] = listener as (...args: unknown[]) => void;
      },
    } as WsLikeSocket);
    // A prelude stating a user's session: admission waits on authenticate.
    listeners.message!(
      new TextEncoder().encode(JSON.stringify({ auth: { backend_session: { user_id: "alice" } } })),
      true,
    );
    const megabyte = new Uint8Array(1024 * 1024);
    for (let sent = 0; sent < 40 && closes.length === 0; sent += 1) {
      listeners.message!(megabyte, true);
      await Promise.resolve();
    }
    expect(closes[0]?.[0]).toBe(1013);
    expect(closes[0]?.[1]).toContain("faster than the server takes");
  });

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

  it("updates a row without reading it first after a restart", async () => {
    const dir = await mkdtemp(join(tmpdir(), "jazz-embedded-"));
    cleanups.push(() => rm(dir, { recursive: true, force: true }));
    const editable = s.definePermissions(app, ({ policy, session }) => {
      policy.todos.allowRead.where({ owner: session.user });
      policy.todos.allowInsert.where({ owner: session.user });
      policy.todos.allowUpdate.where({ owner: session.user });
      policy.todos.allowDelete.never();
    });
    const editableApp = await appWith(editable);
    const open = () =>
      createJazz({
        app: editableApp,
        permissions: editable,
        store: sqlite({ path: dir }),
        authenticate,
      }).ready();
    const alice = () => new Request("http://app/x", { headers: { "x-test-user": "alice" } });

    const first = await open();
    const owner = await first.createAccountFor("alice");
    const row = await (
      await first.forReqUser(alice())
    )
      .insert(app.todos, { title: "draft", owner })
      .wait({ tier: "global" });
    await first.close();

    // A REST PATCH in a fresh process: nothing has read the row here yet.
    const second = await open();
    cleanups.push(() => second.close());
    const asAlice = await second.forReqUser(alice());
    await asAlice.update(app.todos, row.id, { title: "edited" }).wait({ tier: "global" });
    expect((await asAlice.one(app.todos.where({ id: row.id })))?.title).toBe("edited");
    await second.close();

    // The backend's own db too, and a write right behind it keeps its order.
    const third = await open();
    cleanups.push(() => third.close());
    third.db.update(app.todos, row.id, { title: "first" });
    await third.db.update(app.todos, row.id, { title: "second" }).wait({ tier: "global" });
    expect((await third.db.one(app.todos.where({ id: row.id })))?.title).toBe("second");
    // A row the read cannot find is refused as missing.
    await expect(
      third.db.update(app.todos, randomUUID(), { title: "nope" }).wait({ tier: "global" }),
    ).rejects.toThrow(/the row does not exist or the session may not read it/);
  }, 60_000);

  it("routes an unawaited rejection only to the Db of the session that made the write (#4784)", async () => {
    const appId = randomUUID();
    const core = await startLocalJazzServer({ appId, inMemory: true });
    cleanups.push(() => core.stop());
    // Nobody may update a note, so the core refuses every update.
    const { app: notes, permissions: frozen } = await defineShippedApp(
      { notes: s.table({ body: s.string() }, {}) },
      (notes) =>
        s.definePermissions(notes, ({ policy }) => {
          policy.notes.allowRead.always();
          policy.notes.allowInsert.always();
          policy.notes.allowUpdate.never();
        }),
    );
    const jazz = await createJazz({
      app: notes,
      permissions: frozen,
      store: remote({ url: core.url, appId, secret: core.adminSecret }),
      authenticate,
    }).ready();
    cleanups.push(() => jazz.close());
    await jazz.createAccountFor("alice");
    await jazz.createAccountFor("bob");
    const as = (user: string) =>
      jazz.forReqUser(new Request("http://app/x", { headers: { "x-test-user": user } }));
    const note = await jazz.db.insert(notes.notes, { body: "initial" }).wait({ tier: "global" });

    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
    cleanups.push(async () => consoleError.mockRestore());
    // Alice's request registers its handler first.
    const asAlice = await as("alice");
    const seenByAlice = vi.fn();
    asAlice.onMutationError(seenByAlice);
    const asBob = await as("bob");
    const seenByBob = vi.fn();
    asBob.onMutationError(seenByBob);

    // Bob's write is refused, and nobody awaits it.
    const bobsWrite = asBob.update(notes.notes, note.id, { body: "bob" });
    const bobsTxId = await bobsWrite.txId;
    await vi.waitFor(() => expect(seenByBob).toHaveBeenCalledTimes(1), { timeout: 10_000 });
    expect(seenByBob.mock.calls[0]![0]).toMatchObject({
      code: "permission_denied",
      transaction: { transactionId: bobsTxId },
    });
    // Alice, another user, never sees Bob's refusal.
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(seenByAlice).not.toHaveBeenCalled();
    expect(consoleError).not.toHaveBeenCalledWith(
      "Unhandled Jazz mutation error",
      expect.anything(),
    );
  }, 60_000);

  it("leaves an update under a relational read policy to the core, from a fresh backend", async () => {
    const appId = randomUUID();
    const core = await startLocalJazzServer({ appId, inMemory: true });
    cleanups.push(() => core.stop());
    // Access to a note comes from other rows, which a fresh backend has not loaded.
    const { app: shared, permissions: relational } = await defineShippedApp(
      {
        readers: s.table({ user: s.uuid() }, {}),
        editors: s.table({ user: s.uuid() }, {}),
        notes: s.table({ body: s.string() }, {}),
      },
      (shared) =>
        s.definePermissions(shared, ({ policy, session }) => {
          policy.readers.allowRead.where({ user: session.user });
          policy.editors.allowRead.where({ user: session.user });
          policy.notes.allowRead.where(() => policy.readers.exists.where({ user: session.user }));
          policy.notes.allowUpdate.where(() => policy.editors.exists.where({ user: session.user }));
        }),
    );
    const open = () =>
      createJazz({
        app: shared,
        permissions: relational,
        store: remote({ url: core.url, appId, secret: core.adminSecret }),
        authenticate,
      }).ready();
    const as = (jazz: Jazz, user: string) =>
      jazz.forReqUser(new Request("http://app/x", { headers: { "x-test-user": user } }));

    const first = await open();
    const alice = await first.createAccountFor("alice");
    const bob = await first.createAccountFor("bob");
    await first.createAccountFor("carol");
    await first.db.insert(shared.readers, { user: alice }).wait({ tier: "global" });
    await first.db.insert(shared.readers, { user: bob }).wait({ tier: "global" });
    await first.db.insert(shared.editors, { user: alice }).wait({ tier: "global" });
    const note = await first.db.insert(shared.notes, { body: "initial" }).wait({ tier: "global" });
    await first.close();

    // Each session writes from a backend of its own that has loaded nothing,
    // not even the grants, so each update reads the note through the retry
    // path first, and the core decides.
    const fresh = async (user: string) => {
      const jazz = await open();
      cleanups.push(() => jazz.close());
      return as(jazz, user);
    };
    // An editor's update goes through.
    const asAlice = await fresh("alice");
    await asAlice.update(shared.notes, note.id, { body: "alice" }).wait({ tier: "global" });
    // A reader may read the note but not update it: the backend stages the
    // update it can't decide, the core refuses it, and the wait says so.
    const asBob = await fresh("bob");
    await expect(
      asBob.update(shared.notes, note.id, { body: "bob" }).wait({ tier: "global" }),
    ).rejects.toThrow(/permission_denied|Write rejected by server authorization/);
    // A session the core won't show the note to is refused as well.
    const asCarol = await fresh("carol");
    await expect(
      asCarol.update(shared.notes, note.id, { body: "carol" }).wait({ tier: "global" }),
    ).rejects.toMatchObject({ code: "permission_denied" });
    // The backend's own read, which no session policy filters.
    const check = await open();
    cleanups.push(() => check.close());
    expect(
      (await check.db.one(shared.notes.where({ id: note.id }), { tier: "remote" }))?.body,
    ).toBe("alice");
  }, 60_000);

  it("refuses a row the backend holds but the session may not read with a missing one's code", async () => {
    const appId = randomUUID();
    const core = await startLocalJazzServer({ appId, inMemory: true });
    cleanups.push(() => core.stop());
    const owned = s.definePermissions(app, ({ policy, session }) => {
      policy.todos.allowRead.where({ owner: session.user });
      policy.todos.allowUpdate.where({ owner: session.user });
      policy.todos.allowDelete.where({ owner: session.user });
    });
    const jazz = await createJazz({
      app: await appWith(owned),
      permissions: owned,
      store: remote({ url: core.url, appId, secret: core.adminSecret }),
      authenticate,
    }).ready();
    cleanups.push(() => jazz.close());
    await jazz.createAccountFor("bob");
    // The backend's own writes: it holds both rows, live and deleted.
    const hidden = await jazz.db
      .insert(app.todos, { title: "alice's", owner: randomUUID() })
      .wait({ tier: "global" });
    const gone = await jazz.db
      .insert(app.todos, { title: "alice's, deleted", owner: randomUUID() })
      .wait({ tier: "global" });
    await jazz.db.delete(app.todos, gone.id).wait({ tier: "global" });
    const bob = await jazz.forReqUser(
      new Request("http://app/x", { headers: { "x-test-user": "bob" } }),
    );
    const refusal = async (id: string) => {
      try {
        await bob.update(app.todos, id, { title: "bob's" }).wait({ tier: "global" });
      } catch (error) {
        const { message, code } = error as Error & { code?: string };
        return { message, code };
      }
      throw new Error("expected the update to be refused");
    };
    const missing = await refusal(randomUUID());
    expect(missing.code).toBe("permission_denied");
    // The backend stages an update of a row it holds and the core refuses
    // it, with the code it refuses a missing row with. A deleted row can't be
    // staged: the backend refuses it at once, as a missing one.
    expect((await refusal(hidden.id)).code).toBe("permission_denied");
    expect(await refusal(gone.id)).toEqual(missing);
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
    // An account one instance creates is the account every instance finds.
    const bobAccount = await jazz.createAccountFor("bob");
    const bobRequest = new Request("http://app/x", { headers: { "x-test-user": "bob" } });
    await expect(other.accountFor(bobRequest)).resolves.toBe(bobAccount);
    const asBob = await other.forReqUser(bobRequest);
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
      initial: "device",
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
    const alice = await jazz.createAccountFor("alice");
    await (
      await asAlice()
    )
      .insert(app.todos, { title: "before", owner: alice })
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
      db.insert(app.todos, { title: "after", owner: alice }).wait({ tier: "global" }),
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

  it("closes the connections its routes are asked on once it has closed", async () => {
    const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    const viaFetch = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    // One host mounts nodeHandler, the other a Fetch handler (Next's route
    // handlers). That Next passes the header on, and so lets `next start`
    // stop, was checked by hand against a real `next start` (#4900).
    const servers = [
      createServer((req, res) => jazz.nodeHandler(req, res)),
      createServer(async (req, res) => {
        const response = await viaFetch.handle(
          new Request(`http://${req.headers.host}${req.url}`, { method: req.method }),
        );
        res.writeHead(response.status, Object.fromEntries(response.headers));
        res.end(Buffer.from(await response.arrayBuffer()));
      }),
    ];
    await jazz.attach(servers[0]!);
    for (const server of servers) {
      await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
      cleanups.push(
        () =>
          new Promise((resolve) => {
            server.closeAllConnections();
            server.close(resolve);
          }),
      );
    }
    await jazz.close();
    await viaFetch.close();
    const errors = vi.spyOn(console, "error").mockImplementation(() => undefined);
    cleanups.push(async () => errors.mockRestore());
    const agent = new HttpAgent({ keepAlive: true });
    cleanups.push(async () => agent.destroy());
    for (const server of servers) {
      const port = (server.address() as AddressInfo).port;
      // A browser re-opening its sync stream over a kept-alive connection.
      const answer = await new Promise<{ status?: number; connection?: string }>(
        (resolve, reject) => {
          const req = httpRequest(
            { port, host: "127.0.0.1", path: "/jazz/apps/a1/stream", method: "POST", agent },
            (res) => {
              res.resume();
              resolve({ status: res.statusCode, connection: res.headers.connection });
            },
          );
          req.on("error", reject);
          req.end();
        },
      );
      expect(answer).toEqual({ status: 503, connection: "close" });
    }
    // A socket asked for after the close is refused the same way.
    const socketAnswer = await new Promise<{ status?: number; connection?: string }>(
      (resolve, reject) => {
        const req = httpRequest({
          host: "127.0.0.1",
          port: (servers[0]!.address() as AddressInfo).port,
          path: "/jazz/apps/a1/ws",
          headers: {
            connection: "Upgrade",
            upgrade: "websocket",
            "sec-websocket-version": "13",
            "sec-websocket-key": randomBytesBase64(),
          },
        });
        req.on("response", (res) => {
          res.resume();
          resolve({ status: res.statusCode, connection: res.headers.connection });
        });
        req.on("upgrade", () => resolve({ status: 101 }));
        req.on("error", reject);
        req.end();
      },
    );
    expect(socketAnswer).toEqual({ status: 503, connection: "close" });
    // A closed Jazz answering 503 is how shutdown goes, not an error to log.
    expect(errors).not.toHaveBeenCalled();
  });

  it("keeps its sync sockets from the server's other upgrade listeners", async () => {
    const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    cleanups.push(() => jazz.close());
    const server: Server = createServer((req, res) => jazz.nodeHandler(req, res));
    await jazz.attach(server);
    // What a custom Next.js server adds on its first request: a listener
    // that ends every upgrade it doesn't route itself.
    const others: string[] = [];
    server.on("upgrade", (req: IncomingMessage, socket: Duplex) => {
      others.push(req.url ?? "");
      socket.destroy();
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    cleanups.push(
      () =>
        new Promise((resolve) => {
          server.closeAllConnections();
          server.close(resolve);
        }),
    );
    const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    const alice = await userSession(jazz.appId, origin, "alice");
    await alice.db
      .insert(app.todos, { title: "over the socket", owner: alice.session!.user! })
      .wait({ tier: "global" });
    expect(others).toEqual([]);
    // Upgrades for the app's own paths still reach its listener.
    await new Promise<void>((resolve) => {
      const req = httpRequest(`${origin}/_next/webpack-hmr`, {
        headers: { connection: "Upgrade", upgrade: "websocket" },
      });
      req.on("error", () => resolve());
      req.on("close", () => resolve());
      req.end();
    });
    expect(others).toEqual(["/_next/webpack-hmr"]);
    // So does one only looking like Jazz's, and a malformed one doesn't
    // take the process down.
    const upgrade = (path: string) =>
      new Promise<void>((resolve) => {
        const req = httpRequest({
          host: "127.0.0.1",
          port: (server.address() as AddressInfo).port,
          path,
          headers: { connection: "Upgrade", upgrade: "websocket" },
        });
        req.on("error", () => resolve());
        req.on("close", () => resolve());
        req.end();
      });
    await upgrade("/jazz/apps/a/b/ws");
    await upgrade("http://[/jazz/apps/a/ws");
    // A Host header no URL can be made of gets a 400, not a crash.
    const badHost = await new Promise<number | undefined>((resolve, reject) => {
      const req = httpRequest({
        host: "127.0.0.1",
        port: (server.address() as AddressInfo).port,
        path: "/jazz/apps/a/ws",
        headers: { host: "[", connection: "Upgrade", upgrade: "websocket" },
      });
      req.on("response", (res) => resolve(res.statusCode));
      req.on("error", reject);
      req.end();
    });
    expect(badHost).toBe(400);
    // And a plain request with a URL like that passes on (here: 404).
    const badUrl = await new Promise<number | undefined>((resolve, reject) => {
      const req = httpRequest(
        { host: "127.0.0.1", port: (server.address() as AddressInfo).port, path: "http://[/x" },
        (res) => {
          res.resume();
          resolve(res.statusCode);
        },
      );
      req.on("error", reject);
      req.end();
    });
    expect(badUrl).toBe(404);
    expect(others).toEqual(["/_next/webpack-hmr", "/jazz/apps/a/b/ws", "http://[/jazz/apps/a/ws"]);
    // Attaching again changes nothing.
    await jazz.attach(server);
    await upgrade("/_next/again");
    expect(others.at(-1)).toBe("/_next/again");
  }, 30_000);

  it("refuses to attach to an HTTP/2 server, which has no WebSocket upgrades", async () => {
    const jazz = createJazz({ app, permissions, store: memory(), authenticate });
    cleanups.push(() => jazz.close());
    const h2 = createHttp2Server();
    await expect(jazz.attach(h2)).rejects.toThrow(/an HTTP\/2 server has no WebSocket upgrades/);
    // Over TLS, only one that also serves HTTP/1 can upgrade.
    await expect(jazz.attach(createSecureHttp2Server())).rejects.toThrow(
      /an HTTP\/2 server has no WebSocket upgrades/,
    );
    await jazz.attach(createSecureHttp2Server({ allowHTTP1: true }));
  });

  // #5053: in development the browser can't see the server log, so a Jazz
  // route's 503 says why the store did not open (and only there).
  it("says why its store did not open in a 503, in development only", async () => {
    const appId = randomUUID();
    const probe = createServer();
    await new Promise<void>((resolve) => probe.listen(0, "127.0.0.1", resolve));
    const corePort = (probe.address() as AddressInfo).port;
    await new Promise((resolve) => probe.close(resolve));
    const coreUrl = `http://127.0.0.1:${corePort}`;
    const errors = vi.spyOn(console, "error").mockImplementation(() => undefined);
    cleanups.push(async () => errors.mockRestore());
    const answer = async (env: string) => {
      vi.stubEnv("NODE_ENV", env);
      cleanups.push(async () => vi.unstubAllEnvs());
      const jazz = createJazz({
        app,
        permissions,
        store: remote({ url: coreUrl, appId, secret: "down-admin" }),
        authenticate,
      });
      cleanups.push(() => jazz.close());
      const response = await jazz.handle(
        new Request(`http://app.test/jazz/apps/${appId}/ws`, { method: "POST" }),
      );
      return {
        status: response.status,
        header: response.headers.get("x-jazz-store-error"),
        body: await response.text(),
      };
    };

    const dev = await answer("development");
    expect(dev.status).toBe(503);
    expect(dev.header).toBe("1");
    expect(dev.body).toMatch(
      /^Jazz is unavailable; try again shortly: the store did not open: .*could not reach the Jazz core/,
    );
    const prod = await answer("production");
    expect(prod).toEqual({
      status: 503,
      header: null,
      body: "Jazz is unavailable; try again shortly",
    });
  }, 30_000);

  it("attaches with its remote core down at start, names the core, and serves once it is up", async () => {
    const appId = randomUUID();
    const adminSecret = `down-admin-${randomUUID().slice(0, 8)}`;
    // A port nothing listens on yet: the core starts there later.
    const probe = createServer();
    await new Promise<void>((resolve) => probe.listen(0, "127.0.0.1", resolve));
    const corePort = (probe.address() as AddressInfo).port;
    await new Promise((resolve) => probe.close(resolve));
    const coreUrl = `http://127.0.0.1:${corePort}`;
    const jazz = createJazz({
      app,
      permissions,
      store: remote({ url: coreUrl, appId, secret: adminSecret }),
      authenticate,
    });
    cleanups.push(() => jazz.close());
    const server: Server = createServer((req, res) => jazz.nodeHandler(req, res));
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    cleanups.push(
      () =>
        new Promise((resolve) => {
          server.closeAllConnections();
          server.close(resolve);
        }),
    );
    const errors = vi.spyOn(console, "error").mockImplementation(() => undefined);
    cleanups.push(async () => errors.mockRestore());

    // The host's startup goes on: attach() doesn't open the store.
    await jazz.attach(server);
    const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;

    // Jazz routes answer 503 instead of a bare 500. The log names the core;
    // the client isn't told internal addresses.
    const signIn = await fetch(`${origin}/jazz/auth/sign-in`, { method: "POST" });
    expect(signIn.status).toBe(503);
    expect(Number(signIn.headers.get("retry-after"))).toBeGreaterThanOrEqual(1);
    const body = await signIn.text();
    expect(body).toBe("Jazz is unavailable; try again shortly");
    expect(body).not.toContain(coreUrl);
    expect(errors.mock.calls.flat().map(String).join(" ")).toContain(
      `could not reach the Jazz core at ${coreUrl}`,
    );
    // So does a socket.
    const upgraded = await new Promise<number | undefined>((resolve, reject) => {
      const req = httpRequest(`${origin}/jazz/apps/${appId}/ws`, {
        headers: {
          connection: "Upgrade",
          upgrade: "websocket",
          "sec-websocket-version": "13",
          "sec-websocket-key": randomBytesBase64(),
        },
      });
      req.on("response", (res) => resolve(res.statusCode));
      req.on("upgrade", () => resolve(101));
      req.on("error", reject);
      req.end();
    });
    expect(upgraded).toBe(503);
    await expect(jazz.createAccountFor("alice")).rejects.toThrow(
      new RegExp(`could not reach the Jazz core at ${coreUrl} \\(ECONNREFUSED\\)`),
    );

    // The core comes up: the next use past the backoff opens the store.
    const core = await startLocalJazzServer({ appId, port: corePort, inMemory: true, adminSecret });
    cleanups.push(() => core.stop());
    const deadline = Date.now() + 30_000;
    let alice: string | undefined;
    while (!alice) {
      alice = await jazz.createAccountFor("alice").catch(async (error: unknown) => {
        if (Date.now() > deadline) throw error;
        await new Promise((resolve) => setTimeout(resolve, 500));
        return undefined;
      });
    }
    const db = await jazz.forReqUser(
      new Request("http://app/x", { headers: { "x-test-user": "alice" } }),
    );
    await db.insert(app.todos, { title: "after", owner: alice }).wait({ tier: "global" });
    // Sockets upgrade now: attach() took effect once the store opened.
    const user = await userSession(jazz.appId, origin, "bob");
    await user.db
      .insert(app.todos, { title: "synced", owner: user.session!.user! })
      .wait({ tier: "global" });
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
    const alice = await jazz.createAccountFor("alice");
    const db = await jazz.forReqUser(request);
    await db.insert(app.todos, { title: "reached", owner: alice }).wait({ tier: "global" });

    await core.stop();
    db.insert(app.todos, { title: "stranded", owner: alice });

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
    const newer = await createJazz({
      app: await appWith(noInserts),
      permissions: noInserts,
      store,
      authenticate,
    }).ready();
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
        initial: "device",
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
    const { app: latest, permissions: latestPermissions } = await defineShippedApp(
      {
        todos: s.table({ title: s.string(), owner: s.uuid() }, {}).history("latest"),
      },
      (latest) =>
        s.definePermissions(latest, ({ policy, session }) => {
          policy.todos.allowRead.where({ owner: session.user });
          policy.todos.allowInsert.where({ owner: session.user });
          policy.todos.allowUpdate.never();
          policy.todos.allowDelete.never();
        }),
    );
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
