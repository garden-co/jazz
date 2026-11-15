import { randomUUID } from "node:crypto";
import { createServer, request as httpRequest, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { generateSecret, identityOf, signChallenge, signToken } from "../local-first-auth/index.js";
import { localFirstAccountId } from "../accounts/local-first.js";
import { defineShippedApp } from "../testing/client-migrations.js";
import { startLocalJazzServer } from "../dev/dev-server.js";
import { schema as s } from "../index.js";
import {
  acceptDeviceAccounts,
  createJazz,
  createJazzClient,
  memory,
  remote,
  type Jazz,
} from "./index.js";

// `forReqUser`, `withAttribution` and `accountFor` on a plain HTTP request
// from a device account (#5500): the request carries the account's
// local-first proof as `Authorization: Bearer` (`db.getLocalFirstIdentityProof()`),
// checked as a sync socket's is, and admitted by the core. `forReqUser`
// takes it, policy-scoped as the device's sync is; `withAttribution` (backend
// authority) and `accountFor` only with `{ deviceAccounts: true }`, since
// anyone can make a device account.

const opt = { deviceAccounts: true } as const;

const app = await defineShippedApp({
  todos: s.table({ title: s.string(), owner: s.uuid() }, {}),
});
const permissions = s.definePermissions(app, ({ policy, session }) => {
  policy.todos.allowRead.where({ owner: session.user });
  policy.todos.allowInsert.where({ owner: session.user });
  policy.todos.allowUpdate.never();
  policy.todos.allowDelete.never();
});

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
});

/** The documented setup: the app's user (a header here), else a device account. */
function authenticate(request: Request) {
  const userId = request.headers.get("x-test-user");
  return userId
    ? { userId, expiresAt: Math.floor(Date.now() / 1000) + 3600 }
    : acceptDeviceAccounts(request);
}

async function serve(jazz: Jazz): Promise<string> {
  const server: Server = createServer((req, res) =>
    jazz.nodeHandler(req, res, () => {
      res.statusCode = 404;
      res.end("app route");
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
  const { port } = server.address() as { port: number };
  return `http://127.0.0.1:${port}`;
}

/** An app route's request, as a browser's `fetch` to `origin` sends it. */
function route(origin: string, headers: Record<string, string> = {}): Request {
  return new Request(`${origin}/api/notes`, { method: "POST", headers });
}

const bearer = (token: string) => ({ authorization: `Bearer ${token}` });

describe("forReqUser for a device account", () => {
  it("acts as the device account a browser's proof names, under its policies", async () => {
    const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz);
    const device = await createJazzClient({
      app,
      permissions,
      serverUrl: `${origin}/jazz`,
      driver: { type: "memory" },
      initial: "device",
    });
    cleanups.push(() => device.close());
    const client = device.getSnapshot().client!;
    const account = client.session!.user!;
    const own = await client.db
      .insert(app.todos, { title: "from the device", owner: account })
      .wait({ tier: "global" });

    // What a browser attaches to its own fetch.
    const proof = client.db.getLocalFirstIdentityProof();
    expect(proof).toEqual(expect.any(String));
    // It is for this app's endpoint only.
    expect(client.db.getLocalFirstIdentityProof({ audience: "https://other.example/jazz" })).toBe(
      null,
    );
    const request = () => route(origin, bearer(proof!));

    // Without the opt-in, withAttribution and accountFor take no device account.
    await expect(jazz.accountFor(request())).resolves.toBeNull();
    await expect(jazz.withAttribution(request())).rejects.toMatchObject({ code: "signed_out" });
    await expect(jazz.accountFor(request(), opt)).resolves.toBe(account);
    const db = await jazz.forReqUser(request());
    // Its policies: it reads its own row and nothing else, writes as itself.
    const someoneElse = await jazz.db
      .insert(app.todos, { title: "not the device's", owner: randomUUID() })
      .wait({ tier: "global" });
    const seen = await db.all(app.todos);
    expect(seen.map((todo) => todo.id)).toEqual([own.id]);
    expect(seen.map((todo) => todo.id)).not.toContain(someoneElse.id);
    const written = await db
      .insert(app.todos, { title: "from a route", owner: account })
      .wait({ tier: "global" });
    const [stamped] = await jazz.db.all(
      app.todos.where({ id: written.id }).select("title", "$createdBy"),
    );
    expect(stamped?.$createdBy).toMatchObject({ account });
    // A write its policy refuses is refused.
    await expect(
      db.insert(app.todos, { title: "for someone else", owner: randomUUID() }).wait({
        tier: "global",
      }),
    ).rejects.toThrow();
    // The device itself sees the route's write.
    await expect
      .poll(async () => (await client.db.all(app.todos)).map((todo) => todo.title).sort(), {
        timeout: 10_000,
      })
      .toEqual(["from a route", "from the device"]);

    // withAttribution: backend authority, stamped as the device account.
    const onBehalf = await jazz.withAttribution(request(), opt);
    const behalf = await onBehalf
      .insert(app.todos, { title: "on its behalf", owner: randomUUID() })
      .wait({ tier: "global" });
    const [byBackend] = await jazz.db.all(
      app.todos.where({ id: behalf.id }).select("title", "$createdBy"),
    );
    expect(byBackend?.$createdBy).toMatchObject({ account });
  }, 60_000);

  it("takes an app-id proof too, on the account sync founds for it", async () => {
    const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    cleanups.push(() => jazz.close());
    const secret = generateSecret();
    const token = signToken(secret, { audience: jazz.appId });
    const account = localFirstAccountId(jazz.appId, identityOf(secret).subject);
    await expect(jazz.accountFor(route("http://app", bearer(token)), opt)).resolves.toBe(account);
    const db = await jazz.forReqUser(route("http://app", bearer(token)));
    const row = await db.insert(app.todos, { title: "t", owner: account }).wait({ tier: "global" });
    const [stamped] = await jazz.db.all(app.todos.where({ id: row.id }).select("$createdBy"));
    expect(stamped?.$createdBy).toMatchObject({ account });
  });

  it("refuses a tampered, expired, foreign or missing proof", async () => {
    const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
    cleanups.push(() => jazz.close());
    const origin = "http://127.0.0.1:4000";
    const endpoint = `${origin}/jazz`;
    const secret = generateSecret();
    const valid = signToken(secret, { audience: endpoint });
    await expect(jazz.accountFor(route(origin, bearer(valid)), opt)).resolves.toEqual(
      expect.any(String),
    );

    const [header, claims, signature] = valid.split(".") as [string, string, string];
    // Another subject under the same signature.
    const forgedClaims = Buffer.from(
      JSON.stringify({
        ...JSON.parse(Buffer.from(claims, "base64url").toString()),
        sub: identityOf(generateSecret()).subject,
      }),
    ).toString("base64url");
    const flipped = `${signature.slice(0, -2)}${signature.endsWith("AA") ? "BB" : "AA"}`;
    const nowSeconds = Math.floor(Date.now() / 1000);
    const refused = {
      "a tampered payload": `${header}.${forgedClaims}.${signature}`,
      "a tampered signature": `${header}.${claims}.${flipped}`,
      "an expired proof": signToken(secret, {
        audience: endpoint,
        ttlSeconds: 60,
        now: nowSeconds - 3600,
      }),
      // A guest of another endpoint, as sync refuses it.
      "another endpoint's proof": signToken(secret, { audience: "https://other.example/jazz" }),
      "another app's proof": signToken(secret, { audience: randomUUID() }),
    };
    for (const [what, token] of Object.entries(refused)) {
      const calls = [
        (request: Request) => jazz.forReqUser(request),
        (request: Request) => jazz.withAttribution(request, opt),
        (request: Request) => jazz.accountFor(request, opt),
      ];
      for (const call of calls) {
        await expect(call(route(origin, bearer(token))), what).rejects.toMatchObject({
          name: "RequestAuthenticationError",
          code: "invalid_device_proof",
        });
      }
    }
    // The page's origin only counts when it is this host or an allowed one.
    await expect(
      jazz.forReqUser(
        route(origin, {
          ...bearer(signToken(secret, { audience: "https://evil.example/jazz" })),
          origin: "https://evil.example",
        }),
      ),
    ).rejects.toMatchObject({ code: "invalid_device_proof" });

    // No proof: signed out, as before.
    for (const headers of [{}, { authorization: "Bearer not-a-jwt" }] as Record<string, string>[]) {
      await expect(jazz.forReqUser(route(origin, headers))).rejects.toMatchObject({
        name: "RequestAuthenticationError",
        code: "signed_out",
      });
      await expect(jazz.withAttribution(route(origin, headers), opt)).rejects.toMatchObject({
        code: "signed_out",
      });
      await expect(jazz.accountFor(route(origin, headers), opt)).resolves.toBeNull();
    }
  });

  it("refuses a valid proof when authenticate doesn't accept device accounts", async () => {
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: () => null,
    }).ready();
    cleanups.push(() => jazz.close());
    const token = signToken(generateSecret(), { audience: jazz.appId });
    await expect(jazz.forReqUser(route("http://app", bearer(token)))).rejects.toMatchObject({
      code: "signed_out",
    });
    await expect(
      jazz.withAttribution(route("http://app", bearer(token)), opt),
    ).rejects.toMatchObject({ code: "signed_out" });
    await expect(jazz.accountFor(route("http://app", bearer(token)), opt)).resolves.toBeNull();
  });

  it("lets the user authenticate names win, and refuses a key its link revoked", async () => {
    const jazz = await createJazz({
      app,
      permissions,
      store: memory(),
      authenticate,
      guestKeyAfterLink: "revoke",
    }).ready();
    cleanups.push(() => jazz.close());
    const origin = await serve(jazz);
    const secret = generateSecret();
    const token = signToken(secret, { audience: jazz.appId });
    const guestAccount = await jazz.accountFor(route(origin, bearer(token)), opt);
    expect(guestAccount).toEqual(expect.any(String));

    // Bob signs in elsewhere; a request carrying his credential and the
    // device's proof is his, never the device account's, in all three.
    const bob = await jazz.createAccountFor("bob");
    const bobWithProof = () => route(origin, { ...bearer(token), "x-test-user": "bob" });
    await expect(jazz.accountFor(bobWithProof(), opt)).resolves.toBe(bob);
    const asBob = await jazz.forReqUser(bobWithProof());
    const bobs = await asBob.insert(app.todos, { title: "bob's", owner: bob }).wait({
      tier: "global",
    });
    // Bob's db acts under bob's policies: the device account's rows aren't his.
    await expect(
      asBob.insert(app.todos, { title: "as the device", owner: guestAccount! }).wait({
        tier: "global",
      }),
    ).rejects.toThrow();
    const onBobsBehalf = await jazz.withAttribution(bobWithProof(), opt);
    const behalf = await onBobsBehalf
      .insert(app.todos, { title: "on bob's behalf", owner: randomUUID() })
      .wait({ tier: "global" });
    for (const { id } of [bobs, behalf]) {
      const [row] = await jazz.db.all(app.todos.where({ id }).select("$createdBy"));
      expect(row?.$createdBy).toMatchObject({ account: bob });
    }

    // The device account links to alice, who signs up; its key is revoked.
    const post = (path: string, body?: unknown) =>
      fetch(`${origin}/jazz/apps/${jazz.appId}/accounts/${path}`, {
        method: "POST",
        headers: { "x-test-user": "alice", "content-type": "application/json" },
        body: body === undefined ? undefined : JSON.stringify(body),
      });
    const { challenge } = (await (await post("backend-link/challenge")).json()) as {
      challenge: string;
    };
    const linked = await post("backend-link", {
      challenge,
      proof: signChallenge(secret, challenge),
    });
    expect(linked.status).toBe(200);
    expect(((await linked.json()) as { account: string }).account).toBe(guestAccount);
    const revoked = { name: "RequestAuthenticationError", code: "device_account_revoked" };
    await expect(jazz.forReqUser(route(origin, bearer(token)))).rejects.toMatchObject(revoked);
    await expect(jazz.withAttribution(route(origin, bearer(token)), opt)).rejects.toMatchObject(
      revoked,
    );
    await expect(jazz.accountFor(route(origin, bearer(token)), opt)).rejects.toMatchObject(revoked);
    // Without the opt-in, accountFor doesn't look at the proof at all.
    await expect(jazz.accountFor(route(origin, bearer(token)))).resolves.toBeNull();
    // alice herself still acts as the account.
    await expect(jazz.accountFor(route(origin, { "x-test-user": "alice" }))).resolves.toBe(
      guestAccount,
    );
  });

  it("reads only the core's identity_not_authorized refusal as a revoked key", async () => {
    // A core whose account admission answers what the test says, as a
    // misconfigured core (or a proxy in front of it) may.
    const appId = randomUUID();
    const core = await startLocalJazzServer({ appId, inMemory: true });
    cleanups.push(() => core.stop());
    const answer: { status?: number; body?: string } = {};
    const proxy = createServer((request, response) => {
      if (request.url?.endsWith("/admin/accounts/found-local-first") && answer.status) {
        response.statusCode = answer.status;
        response.end(answer.body);
        return;
      }
      const target = new URL(request.url ?? "/", core.url);
      const upstream = httpRequest(
        target,
        { method: request.method, headers: { ...request.headers, host: target.host } },
        (reply) => {
          response.writeHead(reply.statusCode ?? 502, reply.headers);
          reply.pipe(response);
        },
      );
      request.pipe(upstream);
    });
    await new Promise<void>((resolve) => proxy.listen(0, "127.0.0.1", resolve));
    cleanups.push(
      () =>
        new Promise<void>((resolve) => {
          proxy.closeAllConnections();
          proxy.close(() => resolve());
        }),
    );
    const url = `http://127.0.0.1:${(proxy.address() as AddressInfo).port}`;
    const jazz = await createJazz({
      app,
      permissions,
      store: remote({ url, appId, secret: core.adminSecret }),
      authenticate,
    }).ready();
    cleanups.push(() => jazz.close());
    const token = signToken(generateSecret(), { audience: appId });
    const request = () => route("http://app", bearer(token));
    await expect(jazz.accountFor(request(), opt)).resolves.toEqual(expect.any(String));

    // Another 403 is the server's problem, not the caller's key.
    answer.status = 403;
    answer.body = "Admin auth not configured";
    for (const call of [
      () => jazz.forReqUser(request()),
      () => jazz.withAttribution(request(), opt),
      () => jazz.accountFor(request(), opt),
    ]) {
      const error = await call().then(
        () => null,
        (thrown: unknown) => thrown,
      );
      expect(error).toBeInstanceOf(Error);
      expect(error).not.toMatchObject({ name: "RequestAuthenticationError" });
      expect((error as Error).message).toMatch(/found-local-first failed \(403\)/);
    }
    // The core's own refusal of the identity is a revoked key.
    answer.body = "identity_not_authorized";
    await expect(jazz.forReqUser(request())).rejects.toMatchObject({
      name: "RequestAuthenticationError",
      code: "device_account_revoked",
    });
  }, 30_000);
});
