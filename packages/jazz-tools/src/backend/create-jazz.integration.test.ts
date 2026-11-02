import { randomUUID } from "node:crypto";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer, request as httpRequest, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { generateSecret, identityOf, signChallenge, signToken } from "../local-first-auth/index.js";
import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { startLocalJazzServer } from "../dev/dev-server.js";
import { createJazz, createJazzSession, memory, remote, sqlite, type Jazz } from "./index.js";
import { localFirstAccountId } from "../accounts/local-first.js";

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
    // Browser worker transports send `jwt_token: null` next to the session the
    // backend vouches for: that is no token, and the socket is admitted.
    const fromWorker = new NodeWebSocket(`${origin.replace("http", "ws")}/jazz/apps/${appId}/ws`, {
      headers: { "x-test-user": "alice" },
    } as never);
    fromWorker.addEventListener("open", () =>
      fromWorker.send(
        JSON.stringify({
          peer_identity: "worker",
          jwt_token: null,
          backend_session: { issuer: `jazz:app:${appId}`, user_id: "alice" },
          auth: { jwt_token: null, backend_session: { user_id: "alice" } },
        }),
      ),
    );
    const answered = await new Promise<string>((resolve) => {
      fromWorker.addEventListener("message", () => resolve("message"));
      fromWorker.addEventListener("close", (event) => resolve(`closed ${event.code}`));
    });
    fromWorker.close();
    expect(answered).toBe("message");
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
    // API routes can name the request's account without a client credential.
    await expect(
      jazz.accountFor(new Request("http://app/x", { headers: { "x-test-user": "alice" } })),
    ).resolves.toBe(alice.session!.user!.account);
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
    expect(withProvenance?.$createdBy).toMatchObject({ account: alice.session!.user!.account });
    // The user synced over a socket, not the stream fallback.
    expect(requestPaths.some((path) => path.includes("/stream"))).toBe(false);
    // This host upgrades sockets, so a failed socket isn't sent to the stream:
    // the probe gets no stream marker and the client retries the socket.
    const probe = await fetch(`${origin}/jazz/apps/${appId}/ws`);
    expect(probe.status).toBe(426);
    expect(probe.headers.get("x-jazz-stream")).toBeNull();
  }, 60_000);

  it("links only a guest whose proof is for this app or this endpoint", async () => {
    const appId = randomUUID();
    const jazz = await createJazz({ appId, app, permissions, store: memory(), authenticate });
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
    const appId = randomUUID();
    const jazz = await createJazz({
      appId,
      app,
      permissions,
      store: memory(),
      authenticate,
      publicUrl: "https://app.example.com/api/jazz/",
    });
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
    await expect(
      createJazz({ appId, app, permissions, store: memory(), authenticate, publicUrl: "/jazz" }),
    ).rejects.toThrow("not an absolute URL");
  });

  it("refuses sign-in and sync started by a page on another origin", async () => {
    const appId = randomUUID();
    const jazz = await createJazz({
      appId,
      app,
      permissions,
      store: memory(),
      authenticate,
      // Written as a URL, compared as an origin.
      allowedOrigins: ["https://app.example.com/"],
    });
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
              backend_session: { issuer: `jazz:app:${appId}`, user_id: "alice" },
            }),
          );
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
    const appId = randomUUID();
    const signedOut = new Set<string>();
    const jazz = await createJazz({
      appId,
      app,
      permissions,
      store: memory(),
      authenticate: (request) => {
        const user = authenticate(request);
        return user && !signedOut.has(user.userId) ? user : null;
      },
      recheckSeconds: 0.05,
    });
    cleanups.push(() => jazz.close());
    let upgrades = 0;
    const origin = await serve(jazz, { sockets: true, onUpgrade: () => upgrades++ });
    const alice = await userSession(appId, origin, "alice");
    await alice.db
      .insert(app.todos, { title: "signed in", owner: alice.session!.user!.account })
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

  it("reconnects a user whose socket's cookies expired but who is still signed in", async () => {
    const appId = randomUUID();
    // Requests seen before `expiredBefore` carry cookies that have since expired.
    const firstSeen = new WeakMap<Request, number>();
    let expiredBefore = 0;
    const jazz = await createJazz({
      appId,
      app,
      permissions,
      store: memory(),
      authenticate: (request) => {
        if (!firstSeen.has(request)) firstSeen.set(request, Date.now());
        return firstSeen.get(request)! < expiredBefore ? null : authenticate(request);
      },
      recheckSeconds: 0.05,
    });
    cleanups.push(() => jazz.close());
    let upgrades = 0;
    const origin = await serve(jazz, { sockets: true, onUpgrade: () => upgrades++ });
    const alice = await userSession(appId, origin, "alice");
    await alice.db
      .insert(app.todos, { title: "before", owner: alice.session!.user!.account })
      .wait({ tier: "global" });
    const opened = upgrades;
    expiredBefore = Date.now() + 1;
    // The re-check on the old socket fails; the client reconnects with fresh
    // cookies and keeps syncing.
    await expect.poll(() => upgrades, { timeout: 5_000, interval: 50 }).toBeGreaterThan(opened);
    await alice.db
      .insert(app.todos, { title: "after", owner: alice.session!.user!.account })
      .wait({ tier: "global" });
    expect(alice.db.getSyncStatus().link).toEqual({ state: "connected" });
  }, 30_000);

  it("links a second identity to the same account", async () => {
    const appId = randomUUID();
    const jazz = await createJazz({ appId, app, permissions, store: memory(), authenticate });
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: true });

    const guest = await userSession(appId, origin, "guest-device-key");
    const account = guest.session!.user!.account;
    const row = await guest.db
      .insert(app.todos, { title: "made as a guest", owner: account })
      .wait({ tier: "global" });

    // The app's sign-up verified both identities, then links them.
    await expect(jazz.linkIdentity({ from: "guest-device-key", to: "alice" })).resolves.toEqual({
      account,
    });

    const alice = await userSession(appId, origin, "alice");
    expect(alice.session!.user!.account).toBe(account);
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
      const appId = randomUUID();
      const dir = await mkdtemp(join(tmpdir(), "jazz-link-"));
      cleanups.push(() => rm(dir, { recursive: true, force: true }));
      const store = kind === "memory" ? memory() : sqlite({ path: dir });
      const jazz = await createJazz({ appId, app, permissions, store, authenticate });
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
      const session = await createJazzSession({
        ...(scoping === "names the app" ? { appId } : {}),
        app,
        permissions,
        serverUrl: `${origin}/jazz`,
        driver: { type: "memory" },
        initial: "local-first",
      });
      cleanups.push(() => session.close());
      const guest = session.getSnapshot().client!;
      const account = guest.session!.user!.account;
      expect(account).toBe(
        localFirstAccountId(
          scoping === "names the app" ? appId : `${origin}/jazz`,
          guest.session!.user!.identity.subject,
        ),
      );
      const row = await guest.db
        .insert(app.todos, { title: "made as a guest", owner: account })
        .wait({ tier: "global" });

      // The app's sign-up created "alice"; the backend now names her.
      signInAs(origin, "alice");
      await session.linkWithBackend();
      const alice = session.getSnapshot().client!;
      expect(alice.session!.user!.account).toBe(account);
      expect(alice.session!.user!.identity).toEqual({
        issuer: `jazz:app:${appId}`,
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
    const appId = randomUUID();
    const jazz = await createJazz({ appId, app, permissions, store: memory(), authenticate });
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
      const appId = randomUUID();
      const jazz = await createJazz({
        appId,
        app,
        permissions,
        store: memory(),
        authenticate,
        guestKeyAfterLink,
      });
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

  it("revokes the guest key on a retry when the first link's revoke didn't happen", async () => {
    const appId = randomUUID();
    const core = await startLocalJazzServer({ appId, inMemory: true });
    cleanups.push(() => core.stop());
    // Two backends on one core: the first links without revoking, as a
    // link whose revoke failed would leave it; the second revokes.
    const backend = async (guestKeyAfterLink: "keep" | "revoke") => {
      const store = remote({
        url: core.url,
        adminSecret: core.adminSecret,
        backendSecret: core.backendSecret,
      });
      const jazz = await createJazz({
        appId,
        app,
        permissions,
        store,
        authenticate,
        guestKeyAfterLink,
      });
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
      .insert(app.todos, { title: "still over the stream", owner: alice.session!.user!.account })
      .wait({ tier: "global" });
    expect((await jazz.db.all(app.todos.where({ id: later.id }))).length).toBe(1);
  }, 60_000);

  it("sends a failed socket to the HTTP stream only through handlers that can't upgrade", async () => {
    const appId = randomUUID();
    const marker = async (response: Promise<Response>) =>
      (await response).headers.get("x-jazz-stream");
    const probe = () => new Request(`http://app.test/jazz/apps/${appId}/ws`);

    const plain = await createJazz({ appId, app, permissions, store: memory(), authenticate });
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
      appId: randomUUID(),
      app,
      permissions,
      store: memory(),
      authenticate,
      upgradesSockets: true,
    });
    cleanups.push(() => own.close());
    expect(
      await marker(own.handle(new Request(`http://app.test/jazz/apps/${own.appId}/ws`))),
    ).toBeNull();

    // An entry that isn't an origin (it would parse as the opaque "null").
    for (const entry of ["localhost:3000", "app.example.com:443", "not a url"]) {
      await expect(
        createJazz({ appId, app, permissions, store: memory(), allowedOrigins: [entry] }),
      ).rejects.toThrow(/is not an origin/);
    }
  }, 60_000);

  it("opens a sqlite directory in one core at a time, and not on serverless hosts", async () => {
    const appId = randomUUID();
    const dir = await mkdtemp(join(tmpdir(), "jazz-lock-"));
    cleanups.push(() => rm(dir, { recursive: true, force: true }));
    // A lock file left by a process that died (under Docker often our own
    // pid, back after a restart) holds nothing: only the OS lock counts.
    await writeFile(join(dir, "jazz.lock"), `process ${process.pid}`);
    const first = await createJazz({ appId, app, permissions, store: sqlite({ path: dir }) });
    await expect(
      createJazz({ appId, app, permissions, store: sqlite({ path: dir }) }),
    ).rejects.toThrow(/is already open/);
    await first.close();
    // Closed, the directory is free again.
    const second = await createJazz({ appId, app, permissions, store: sqlite({ path: dir }) });
    await second.close();

    const previous = process.env.VERCEL;
    process.env.VERCEL = "1";
    try {
      await expect(
        createJazz({ appId, app, permissions, store: sqlite({ path: dir }) }),
      ).rejects.toThrow(/Use remote\(/);
    } finally {
      if (previous === undefined) delete process.env.VERCEL;
      else process.env.VERCEL = previous;
    }
  }, 60_000);

  it("keeps data in the sqlite directory across restarts", async () => {
    const appId = randomUUID();
    const dir = await mkdtemp(join(tmpdir(), "jazz-embedded-"));
    cleanups.push(() => rm(dir, { recursive: true, force: true }));

    const first = await createJazz({ appId, app, permissions, store: sqlite({ path: dir }) });
    const inserted = await first.db
      .insert(app.todos, { title: "kept", owner: randomUUID() })
      .wait({ tier: "global" });
    await first.close();

    const second = await createJazz({ appId, app, permissions, store: sqlite({ path: dir }) });
    cleanups.push(() => second.close());
    const rows = await second.db.all(app.todos.where({ id: inserted.id }));
    expect(rows.map((todo) => todo.title)).toEqual(["kept"]);
  }, 60_000);

  it("fronts a remote core: browsers reach only the backend, the core trusts it", async () => {
    const appId = randomUUID();
    const core = await startLocalJazzServer({ appId, inMemory: true });
    cleanups.push(() => core.stop());
    const store = remote({
      url: core.url,
      adminSecret: core.adminSecret,
      backendSecret: core.backendSecret,
    });
    const jazz = await createJazz({ appId, app, permissions, store, authenticate });
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz, { sockets: true });

    const alice = await userSession(appId, origin, "alice");
    const row = await alice.db
      .insert(app.todos, { title: "through the backend", owner: alice.session!.user!.account })
      .wait({ tier: "global" });

    // A second backend on the same core (another instance, after a restart)
    // sees the same data and the same accounts.
    const other = await createJazz({ appId, app, permissions, store, authenticate });
    cleanups.push(() => other.close());
    const seen = await other.db.all(app.todos.where({ id: row.id }));
    expect(seen.map((todo) => todo.title)).toEqual(["through the backend"]);
    const asAlice = await other.forRequest(
      new Request("http://app/x", { headers: { "x-test-user": "alice" } }),
    );
    expect((await asAlice.all(app.todos)).map((todo) => todo.title)).toEqual([
      "through the backend",
    ]);
    const asBob = await other.forRequest(
      new Request("http://app/x", { headers: { "x-test-user": "bob" } }),
    );
    expect(await asBob.all(app.todos)).toEqual([]);

    // A guest whose client names no app: the backend vouches for the
    // endpoint its proof is for, and the remote core founds its account.
    const guestSession = await createJazzSession({
      app,
      permissions,
      serverUrl: `${origin}/jazz`,
      driver: { type: "memory" },
      initial: "local-first",
    });
    cleanups.push(() => guestSession.close());
    const guest = guestSession.getSnapshot().client!;
    const made = await guest.db
      .insert(app.todos, { title: "as a guest", owner: guest.session!.user!.account })
      .wait({ tier: "global" });
    expect((await jazz.db.all(app.todos.where({ id: made.id }))).map((todo) => todo.title)).toEqual(
      ["as a guest"],
    );
  }, 60_000);
});
