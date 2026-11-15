import { randomUUID } from "node:crypto";
import { createServer, type Server } from "node:http";
import { afterEach, describe, expect, it } from "vitest";
import { generateSecret, identityOf, signChallenge, signToken } from "../local-first-auth/index.js";
import { localFirstAccountId } from "../accounts/local-first.js";
import { defineShippedApp } from "../testing/client-migrations.js";
import { schema as s } from "../index.js";
import { acceptDeviceAccounts, createJazz, createJazzClient, memory, type Jazz } from "./index.js";

// `forReqUser`, `withAttribution` and `accountFor` on a plain HTTP request
// from a device account (#5500): the request carries the account's
// local-first proof as `Authorization: Bearer` (`db.getLocalFirstIdentityProof()`),
// checked as a sync socket's is, and admitted by the core.

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
    const request = () => route(origin, bearer(proof!));

    await expect(jazz.accountFor(request())).resolves.toBe(account);
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
    const onBehalf = await jazz.withAttribution(request());
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
    await expect(jazz.accountFor(route("http://app", bearer(token)))).resolves.toBe(account);
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
    await expect(jazz.accountFor(route(origin, bearer(valid)))).resolves.toEqual(
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
      for (const call of [jazz.forReqUser, jazz.withAttribution, jazz.accountFor]) {
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
    for (const headers of [{}, { authorization: "Bearer not-a-jwt" }]) {
      await expect(jazz.forReqUser(route(origin, headers))).rejects.toMatchObject({
        name: "RequestAuthenticationError",
        code: "signed_out",
      });
      await expect(jazz.withAttribution(route(origin, headers))).rejects.toMatchObject({
        code: "signed_out",
      });
      await expect(jazz.accountFor(route(origin, headers))).resolves.toBeNull();
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
    await expect(jazz.accountFor(route("http://app", bearer(token)))).resolves.toBeNull();
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
    const guestAccount = await jazz.accountFor(route(origin, bearer(token)));
    expect(guestAccount).toEqual(expect.any(String));

    // Bob signs in elsewhere; a request carrying his credential and the
    // device's proof is his, never the device account's.
    const bob = await jazz.createAccountFor("bob");
    await expect(
      jazz.accountFor(route(origin, { ...bearer(token), "x-test-user": "bob" })),
    ).resolves.toBe(bob);

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
    await expect(jazz.forReqUser(route(origin, bearer(token)))).rejects.toMatchObject({
      name: "RequestAuthenticationError",
      code: "device_account_revoked",
    });
    // alice herself still acts as the account.
    await expect(jazz.accountFor(route(origin, { "x-test-user": "alice" }))).resolves.toBe(
      guestAccount,
    );
  });
});
