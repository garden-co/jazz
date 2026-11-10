import { randomUUID } from "node:crypto";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { createJazz, createJazzClient, memory, trustWorkload, type Jazz } from "./index.js";

// #5055: a backend client whose main backend can't upgrade sync sockets
// (next() on Vercel without the `ws` package answered every upgrade with a
// 500) must sync over the HTTP stream, as a browser does, or say why its
// socket failed: never crash its process.

const app = s.defineApp(
  { todos: s.table({ title: s.string(), owner: s.uuid() }, {}) },
  { migrations: "none" },
);
const permissions = s.definePermissions(app, ({ policy, session }) => {
  policy.todos.allowRead.where({ owner: session.user });
  policy.todos.allowInsert.where({ owner: session.user });
  policy.todos.allowUpdate.never();
  policy.todos.allowDelete.never();
});

const AUDIENCE = "https://app.example.test";
const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
  vi.restoreAllMocks();
});

/** A stand-in OIDC issuer on loopback. */
let issuer: string;
let sign: (sub: string) => Promise<string>;
let issuerServer: Server;
beforeAll(async () => {
  const { publicKey, privateKey } = await generateKeyPair("ES256");
  const jwk = { ...(await exportJWK(publicKey)), kid: "k1", alg: "ES256", use: "sig" };
  issuerServer = createServer((req, res) => {
    res.setHeader("content-type", "application/json");
    if (req.url === "/.well-known/openid-configuration") {
      res.end(JSON.stringify({ issuer, jwks_uri: `${issuer}/jwks` }));
    } else if (req.url === "/jwks") {
      res.end(JSON.stringify({ keys: [jwk] }));
    } else {
      res.statusCode = 404;
      res.end("{}");
    }
  });
  await new Promise<void>((resolve) => issuerServer.listen(0, "127.0.0.1", resolve));
  issuer = `http://127.0.0.1:${(issuerServer.address() as AddressInfo).port}`;
  sign = (sub) =>
    new SignJWT({ sub })
      .setProtectedHeader({ alg: "ES256", kid: "k1" })
      .setIssuer(issuer)
      .setAudience(AUDIENCE)
      .setIssuedAt()
      .setExpirationTime("10m")
      .sign(privateKey);
});
afterAll(() => new Promise((resolve) => issuerServer.close(resolve)));

/**
 * The app's main backend behind a server that answers every socket upgrade
 * with a 500, as next() on Vercel did without `ws`. `socketPath` answers
 * a plain request for the socket path the same way when set to "500" (not
 * a Jazz route, so it says nothing about the stream).
 */
async function backendRefusingUpgrades(
  options: { upgradesSockets?: boolean; socketPath?: "jazz" | "500" } = {},
) {
  const jazz: Jazz = await createJazz({
    app,
    permissions,
    store: memory(),
    authenticate: () => null,
    upgradesSockets: options.upgradesSockets,
    backendClients: [
      trustWorkload({ issuer, audience: AUDIENCE, claims: { sub: "ops" }, as: "backend" }),
    ],
  }).ready();
  cleanups.push(() => jazz.close());
  const upgrades: Array<string | undefined> = [];
  const server = createServer((req, res) => {
    if (options.socketPath === "500" && req.url?.endsWith("/ws")) {
      res.statusCode = 500;
      res.end("Internal Server Error");
      return;
    }
    jazz.nodeHandler(req, res);
  });
  server.on("upgrade", (req, socket) => {
    upgrades.push(req.headers.authorization);
    socket.end(
      "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    );
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
  return { jazz, origin, upgrades };
}

async function backendClient(appId: string, origin: string) {
  const session = await createJazzClient({
    appId,
    app,
    permissions,
    serverUrl: `${origin}/jazz`,
    driver: { type: "memory" },
    credential: () => sign("ops"),
  });
  cleanups.push(() => session.close({ timeoutMs: 0 }));
  const { client, status, error } = session.getSnapshot();
  if (status !== "ready") throw error ?? new Error(status);
  return client!;
}

describe("a backend client whose main backend can't upgrade sockets", () => {
  it("syncs over the HTTP stream when the backend says it serves one", async () => {
    const { jazz, origin, upgrades } = await backendRefusingUpgrades();
    const client = await backendClient(jazz.appId, origin);

    const owner = randomUUID();
    const row = await client.db
      .insert(app.todos, { title: "over the stream", owner })
      .wait({ tier: "global" });
    const [written] = await jazz.db.all(app.todos.where({ id: row.id }));
    expect(written?.title).toBe("over the stream");
    // One socket tried, then the stream: the backend's answer to a plain
    // request for the socket path (426 + x-jazz-stream) says so.
    expect(upgrades).toHaveLength(1);
    expect(upgrades[0]).toMatch(/^Bearer /);
  }, 60_000);

  it("syncs over the HTTP stream after upgrades keep failing at a backend that says it upgrades", async () => {
    // jazz-tools before #5055 on Vercel without `ws`: the backend says it
    // upgrades sockets, and every upgrade gets a 500.
    const { jazz, origin, upgrades } = await backendRefusingUpgrades({ upgradesSockets: true });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const client = await backendClient(jazz.appId, origin);

    const row = await client.db
      .insert(app.todos, { title: "after retries", owner: randomUUID() })
      .wait({ tier: "global" });
    expect((await jazz.db.all(app.todos.where({ id: row.id })))[0]?.title).toBe("after retries");
    // Failed sockets are retried, then the stream takes over, saying why.
    expect(upgrades.length).toBeGreaterThanOrEqual(3);
    expect(warn.mock.calls.flat().join(" ")).toMatch(/doesn't pass WebSocket upgrades on/);
  }, 120_000);

  it("keeps retrying a socket whose upgrade gets a 500 from something else, saying so", async () => {
    // Nothing on the socket path speaks Jazz: no stream to fall back to.
    const { jazz, origin } = await backendRefusingUpgrades({ socketPath: "500" });
    const client = await backendClient(jazz.appId, origin);

    // The first read that needs the server fails with the upgrade's
    // status, and the client keeps trying: nothing escapes as an
    // uncaught exception (vitest fails the run on one).
    const read = client.db.all(app.todos, { tier: "remote" });
    await expect(read).rejects.toThrow(/WebSocket upgrade .* answered with HTTP 500/);
    // Still retrying (connecting until an outage is published, then
    // offline with retrying), never stopped, and the session stays ready.
    const link = client.db.getSyncStatus().link;
    expect(link.state === "connecting" || (link.state === "offline" && link.retrying)).toBe(true);
    await new Promise((resolve) => setTimeout(resolve, 2_000));
    expect(client.db.getSyncStatus().link).not.toMatchObject({ state: "offline", retrying: false });
  }, 60_000);
});
