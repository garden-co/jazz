import { randomUUID } from "node:crypto";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { exportJWK, generateKeyPair, SignJWT, type JWTPayload } from "jose";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { createJazz, createJazzSession, memory, trustWorkload, type Jazz } from "./index.js";

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

const AUDIENCE = "https://app.example.test";
const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
});

/** A stand-in OIDC issuer on loopback: discovery, JWKS and token minting. */
let issuer: string;
let sign: (
  claims: JWTPayload,
  options?: { expiresIn?: string | number; audience?: string; issuer?: string },
) => Promise<string>;
/** When set, the issuer's key set answers 500 (an outage). */
let keysDown = false;
let otherKeySign: (claims: JWTPayload) => Promise<string>;
let issuerServer: Server;
beforeAll(async () => {
  const { publicKey, privateKey } = await generateKeyPair("ES256");
  const other = await generateKeyPair("ES256");
  const jwk = { ...(await exportJWK(publicKey)), kid: "k1", alg: "ES256", use: "sig" };
  issuerServer = createServer((req, res) => {
    res.setHeader("content-type", "application/json");
    if (req.url === "/.well-known/openid-configuration") {
      res.end(JSON.stringify({ issuer, jwks_uri: `${issuer}/jwks` }));
    } else if (req.url === "/jwks" && keysDown) {
      res.statusCode = 500;
      res.end("{}");
    } else if (req.url === "/jwks") {
      res.end(JSON.stringify({ keys: [jwk] }));
    } else {
      res.statusCode = 404;
      res.end("{}");
    }
  });
  await new Promise<void>((resolve) => issuerServer.listen(0, "127.0.0.1", resolve));
  issuer = `http://127.0.0.1:${(issuerServer.address() as AddressInfo).port}`;
  const mint = (
    key: CryptoKey,
    claims: JWTPayload,
    expiresIn: string | number = "10m",
    audience = AUDIENCE,
    tokenIssuer = issuer,
  ) =>
    new SignJWT(claims)
      .setProtectedHeader({ alg: "ES256", kid: "k1" })
      .setIssuer(tokenIssuer)
      .setAudience(audience)
      .setIssuedAt()
      .setExpirationTime(expiresIn)
      .sign(key);
  sign = (claims, options) =>
    mint(privateKey, claims, options?.expiresIn, options?.audience, options?.issuer);
  otherKeySign = (claims) => mint(other.privateKey, claims);
});
afterAll(() => new Promise((resolve) => issuerServer.close(resolve)));

/** The app's own auth: the user named in a header. */
function authenticate(request: Request) {
  const userId = request.headers.get("x-test-user");
  return userId ? { userId } : null;
}

async function serve(jazz: Jazz): Promise<string> {
  const server: Server = createServer((req, res) => jazz.nodeHandler(req, res));
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

async function appWith(backendClients: Parameters<typeof createJazz>[0]["backendClients"]) {
  const appId = randomUUID();
  const jazz = await createJazz({
    appId,
    app,
    permissions,
    store: memory(),
    authenticate,
    backendClients,
  });
  cleanups.push(() => jazz.close());
  return { appId, jazz, origin: await serve(jazz) };
}

const signIn = (origin: string, appId: string, headers: Record<string, string>) =>
  fetch(`${origin}/jazz/apps/${appId}/accounts/backend-sign-in`, { method: "POST", headers });

const scoped = (claims: Record<string, string>) =>
  trustWorkload({
    issuer,
    audience: AUDIENCE,
    claims,
    as: (token) => ({ userId: `machine:${token.sub}`, claims: { machine: true } }),
  });

describe("trustWorkload", () => {
  it("needs claims that name the workload", () => {
    expect(() => trustWorkload({ issuer, audience: AUDIENCE, claims: {}, as: "backend" })).toThrow(
      /claims must name the workload/,
    );
    expect(() =>
      trustWorkload({ issuer, audience: AUDIENCE, claims: { aud: "x" }, as: "backend" }),
    ).toThrow(/checked on its own/);
    expect(() =>
      trustWorkload({
        issuer: "http://issuer.example.test",
        audience: AUDIENCE,
        claims: { sub: "x" },
        as: "backend",
      }),
    ).toThrow(/https/);
  });
});

describe("scoped backend clients", () => {
  it("act as the user their claims map to, and nothing else gets in", async () => {
    const { appId, origin } = await appWith([scoped({ sub: "worker", repository: "acme/app" })]);
    const good = await sign({ sub: "worker", repository: "acme/app" });

    const signedIn = await signIn(origin, appId, { authorization: `Bearer ${good}` });
    expect(signedIn.status).toBe(200);
    expect(await signedIn.json()).toMatchObject({
      identity: { issuer: `jazz:app:${appId}`, subject: "machine:worker" },
      claims: { machine: true },
    });

    const refused = [
      // meant for another audience
      await sign({ sub: "worker", repository: "acme/app" }, { audience: "https://other.test" }),
      // too close to expiry for a session (inside the minimum remaining time)
      await sign(
        { sub: "worker", repository: "acme/app" },
        { expiresIn: Math.floor(Date.now() / 1000) + 30 },
      ),
      // another workload of the same issuer
      await sign({ sub: "worker", repository: "acme/other" }),
      await sign({ sub: "intruder", repository: "acme/app" }),
      // the issuer's claims, but not its key
      await otherKeySign({ sub: "worker", repository: "acme/app" }),
      // expired (past the clock tolerance)
      await sign(
        { sub: "worker", repository: "acme/app" },
        { expiresIn: Math.floor(Date.now() / 1000) - 120 },
      ),
    ];
    for (const token of refused) {
      expect((await signIn(origin, appId, { authorization: `Bearer ${token}` })).status).toBe(401);
    }
    // A refused token from a trusted issuer is never handed to `authenticate`.
    const bad = refused[0]!;
    expect(
      (await signIn(origin, appId, { authorization: `Bearer ${bad}`, "x-test-user": "alice" }))
        .status,
    ).toBe(401);
    // A token from an issuer no trust names is the app's own auth to judge:
    // alone it grants nothing, and it doesn't stop the app's own user.
    const foreign = await sign({ sub: "worker" }, { issuer: "https://elsewhere.test" });
    expect((await signIn(origin, appId, { authorization: `Bearer ${foreign}` })).status).toBe(401);
    expect(
      (await signIn(origin, appId, { authorization: `Bearer ${foreign}`, "x-test-user": "alice" }))
        .status,
    ).toBe(200);
    // The app's own users still sign in as before.
    expect((await signIn(origin, appId, { "x-test-user": "alice" })).status).toBe(200);
  });

  it("sync with the user's permissions", async () => {
    const { appId, jazz, origin } = await appWith([scoped({ sub: "worker" })]);
    let tokens = 0;
    const session = await createJazzSession({
      appId,
      app,
      permissions,
      serverUrl: `${origin}/jazz`,
      driver: { type: "memory" },
      // A fresh token for every request and socket.
      credential: () => (tokens++, sign({ sub: "worker" })),
    });
    cleanups.push(() => session.close());
    // It opened signed in as its user, without being asked to.
    const { client, status, error } = session.getSnapshot();
    if (status !== "ready") throw error ?? new Error(status);
    const account = client!.session!.user!.account;
    expect(client!.session!.user!.identity).toEqual({
      issuer: `jazz:app:${appId}`,
      subject: "machine:worker",
    });

    const row = await client!.db
      .insert(app.todos, { title: "from the worker", owner: account })
      .wait({ tier: "global" });
    expect((await jazz.db.all(app.todos.where({ id: row.id }))).map((t) => t.title)).toEqual([
      "from the worker",
    ]);
    // Its permissions are the user's: a row for someone else is refused.
    await expect(
      client!.db
        .insert(app.todos, { title: "for someone else", owner: randomUUID() })
        .wait({ tier: "global" }),
    ).rejects.toThrow();
    // API routes see the same user.
    const request = new Request("http://app/x", {
      headers: { authorization: `Bearer ${await sign({ sub: "worker" })}` },
    });
    expect(await jazz.accountFor(request)).toBe(account);
    // Sign-in and the socket each asked for a token.
    expect(tokens).toBeGreaterThanOrEqual(2);
  });
});

describe("trusted backend clients", () => {
  it("sync with backend authority", async () => {
    const { appId, jazz, origin } = await appWith([
      trustWorkload({ issuer, audience: AUDIENCE, claims: { sub: "ops" }, as: "backend" }),
    ]);
    // Without its token, the route doesn't exist.
    expect(
      (await fetch(`${origin}/jazz/apps/${appId}/backend/admit`, { method: "POST" })).status,
    ).toBe(404);
    // A token of another workload is refused.
    expect(
      (
        await fetch(`${origin}/jazz/apps/${appId}/backend/admit`, {
          method: "POST",
          headers: { authorization: `Bearer ${await sign({ sub: "intruder" })}` },
        })
      ).status,
    ).toBe(401);

    const session = await createJazzSession({
      appId,
      app,
      permissions,
      serverUrl: `${origin}/jazz`,
      driver: { type: "memory" },
      // The token alone decides: this one opens as a trusted machine.
      credential: () => sign({ sub: "ops" }),
    });
    cleanups.push(() => session.close());
    const { client, status, error } = session.getSnapshot();
    if (status !== "ready") throw error ?? new Error(status);

    // Policies don't apply: it writes rows no user's policy would allow.
    const owner = randomUUID();
    const row = await client!.db
      .insert(app.todos, { title: "from ops", owner })
      .wait({ tier: "global" });
    expect((await jazz.db.all(app.todos.where({ id: row.id }))).map((t) => t.owner)).toEqual([
      owner,
    ]);
    // A trusted machine has no user of its own.
    expect(
      (
        await signIn(origin, appId, {
          authorization: `Bearer ${await sign({ sub: "ops" })}`,
        })
      ).status,
    ).toBe(401);
  });

  it("a credential trusted as neither fails to open, and decides alone", async () => {
    const { appId, origin } = await appWith([
      trustWorkload({ issuer, audience: AUDIENCE, claims: { sub: "ops" }, as: "backend" }),
    ]);
    const base = {
      appId,
      app,
      permissions,
      serverUrl: `${origin}/jazz`,
      driver: { type: "memory" as const },
    };
    await expect(
      createJazzSession({ ...base, credential: () => sign({ sub: "intruder" }) }),
    ).rejects.toThrow(/neither as a trusted machine nor as a user/);
    await expect(
      createJazzSession({
        ...base,
        credential: () => sign({ sub: "ops" }),
        initial: "local-first",
      }),
    ).rejects.toThrow(/leave out initial/);
  });

  it("are refused on the socket with another workload's token", async () => {
    const { appId, origin } = await appWith([
      trustWorkload({ issuer, audience: AUDIENCE, claims: { sub: "ops" }, as: "backend" }),
    ]);
    const socket = new WebSocket(`${origin.replace("http", "ws")}/jazz/apps/${appId}/ws`, {
      headers: { authorization: `Bearer ${await sign({ sub: "intruder" })}` },
    } as never);
    socket.addEventListener("open", () =>
      socket.send(JSON.stringify({ peer_identity: "x", backend_secret: "stand-in" })),
    );
    const code = await new Promise<number>((resolve) =>
      socket.addEventListener("close", (event) => resolve(event.code)),
    );
    expect(code).toBe(1008);
  });

  it("a key outage is a 503, never read as a refusal", async () => {
    const { appId, origin } = await appWith([
      trustWorkload({ issuer, audience: AUDIENCE, claims: { sub: "ops" }, as: "backend" }),
    ]);
    keysDown = true;
    cleanups.push(async () => {
      keysDown = false;
    });
    const response = await fetch(`${origin}/jazz/apps/${appId}/backend/admit`, {
      method: "POST",
      headers: { authorization: `Bearer ${await sign({ sub: "ops" })}` },
    });
    expect(response.status).toBe(503);
  });

  it("can't sync as a backend without its token", async () => {
    const { appId, origin } = await appWith([
      trustWorkload({ issuer, audience: AUDIENCE, claims: { sub: "ops" }, as: "backend" }),
    ]);
    const socket = new WebSocket(`${origin.replace("http", "ws")}/jazz/apps/${appId}/ws`);
    socket.addEventListener("open", () =>
      socket.send(JSON.stringify({ peer_identity: "x", backend_secret: "guess" })),
    );
    const code = await new Promise<number>((resolve) =>
      socket.addEventListener("close", (event) => resolve(event.code)),
    );
    expect(code).toBe(1008);
  });
});
