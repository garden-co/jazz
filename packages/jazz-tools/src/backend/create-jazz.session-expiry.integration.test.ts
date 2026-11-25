import { describe, expect, it } from "vitest";
import { randomUUID } from "node:crypto";
import { testPermissions } from "../testing/index.js";
import { startLocalJazzServer } from "../dev/dev-server.js";
import { createJazz, createJazzClient, memory, remote } from "./index.js";
import {
  app,
  authenticate,
  cleanups,
  inAnHour,
  inSeconds,
  permissions,
  requestPaths,
  runCleanupsAfterEach,
  serve,
  signInAs,
  userSession,
} from "./fixtures/create-jazz.js";

runCleanupsAfterEach();

describe("createJazz: ending and re-authenticating sessions", () => {
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

  // The HTTP stream hands the revoke's auth_failed and its close over in one
  // read: the client went offline: auth on a socket, but read the stream's
  // close as a dropped connection and kept reopening (refused each time).
  it("ends a user's open HTTP stream session at once on revokeUser, as a refusal", async () => {
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
    const origin = await serve(jazz, { sockets: false });
    // Same-origin, as a page is: the client falls back to the stream.
    const previous = (globalThis as { location?: unknown }).location;
    (globalThis as { location?: unknown }).location = {
      host: new URL(origin).host,
      href: `${origin}/`,
    };
    cleanups.push(async () => {
      (globalThis as { location?: unknown }).location = previous;
    });
    const alice = await userSession(jazz.appId, origin, "alice");
    await alice.db
      .insert(app.todos, { title: "signed in", owner: alice.session!.user! })
      .wait({ tier: "global" });
    const streamPath = `/jazz/apps/${jazz.appId}/stream`;
    expect(requestPaths).toContain(streamPath);

    signedOut.add("alice");
    expect(await jazz.revokeUser("alice")).toBe(1);
    await expect
      .poll(() => alice.db.getSyncStatus().link, { timeout: 5_000, interval: 20 })
      .toMatchObject({ state: "offline", reason: "auth" });
    // A refusal: the client stops opening streams.
    const opened = requestPaths.filter((path) => path === streamPath).length;
    await new Promise((resolve) => setTimeout(resolve, 2_000));
    expect(requestPaths.filter((path) => path === streamPath).length).toBe(opened);
    expect(alice.db.getSyncStatus().link).toMatchObject({ state: "offline", reason: "auth" });
  }, 30_000);

  it("ends a user's sessions on other instances on revokeUser (serverless)", async () => {
    // Three instances of one app on one core, as on a serverless host: one
    // holds Alice's socket (and Bob's), one her HTTP stream, and the sign-out
    // handler runs on the third.
    const appId = randomUUID();
    const core = await startLocalJazzServer({ appId, inMemory: true });
    cleanups.push(() => core.stop());
    const signedOut = new Set<string>();
    const instance = async () => {
      const jazz = await createJazz({
        app,
        permissions,
        store: remote({ url: core.url, appId, secret: core.adminSecret }),
        authenticate: (request) => {
          const userId = request.headers.get("x-test-user");
          return userId && !signedOut.has(userId) ? { userId, expiresAt: inAnHour() } : null;
        },
      }).ready();
      cleanups.push(() => jazz.close());
      return jazz;
    };
    const [socketHost, streamHost, signOutHandler] = await Promise.all([
      instance(),
      instance(),
      instance(),
    ]);
    const socketOrigin = await serve(socketHost, { sockets: true });
    const streamOrigin = await serve(streamHost, { sockets: false });
    // The page is on the stream host's origin, so its client syncs over the
    // HTTP stream there.
    const previous = (globalThis as { location?: unknown }).location;
    (globalThis as { location?: unknown }).location = {
      host: new URL(streamOrigin).host,
      href: `${streamOrigin}/`,
    };
    cleanups.push(async () => {
      (globalThis as { location?: unknown }).location = previous;
    });
    const viaStream = await userSession(appId, streamOrigin, "alice");
    const viaSocket = await userSession(appId, socketOrigin, "alice");
    for (const client of [viaStream, viaSocket])
      await client.db
        .insert(app.todos, { title: "signed in", owner: client.session!.user! })
        .wait({ tier: "global" });
    expect(requestPaths.some((path) => path.endsWith(`/apps/${appId}/stream`))).toBe(true);

    signedOut.add("alice");
    // None of Alice's sessions is on this instance.
    expect(await signOutHandler.revokeUser("alice")).toBe(0);
    await expect
      .poll(() => viaSocket.db.getSyncStatus().link, { timeout: 5_000, interval: 20 })
      .toMatchObject({ state: "offline", reason: "auth" });
    // The stream host ended Alice's stream session as revoked: her stream
    // client reads that as a refusal too, as a socket client does.
    await expect
      .poll(() => viaStream.db.getSyncStatus().link, { timeout: 5_000, interval: 20 })
      .toMatchObject({ state: "offline", reason: "auth" });
    const late = viaStream.db.insert(app.todos, {
      title: "after revocation",
      owner: viaStream.session!.user!,
    });
    await expect(late.wait({ tier: "global", timeoutMs: 1_000 })).rejects.toThrow();

    // Signing in again afterwards works, on any instance, and so does another
    // user on the instance that held Alice's socket.
    signedOut.delete("alice");
    const bob = await userSession(appId, socketOrigin, "bob");
    await bob.db
      .insert(app.todos, { title: "unaffected", owner: bob.session!.user! })
      .wait({ tier: "global" });
  }, 60_000);

  it("never takes row data for the core's revocation", async () => {
    // Everyone reads every todo, so Bob's text reaches Alice's session.
    const shared = {
      format: 1,
      tables: { todos: { read: "true", insert: "owner = session.user" } },
    };
    const jazz = await createJazz({
      app,
      permissions: shared,
      store: memory(),
      authenticate,
    }).ready();
    cleanups.push(() => jazz.close());
    const alice = await userSession(jazz.appId, await serve(jazz, { sockets: true }), "alice");
    const bob = await userSession(jazz.appId, await serve(jazz, { sockets: true }), "bob");
    // Exactly the core's message for a revoked session, as a row value.
    const forged = "session revoked: jazz.revokeUser ended this user's sessions";
    await bob.db
      .insert(app.todos, { title: forged, owner: bob.session!.user! })
      .wait({ tier: "global" });
    await expect
      .poll(async () => (await alice.db.all(app.todos, { tier: "remote" })).map((t) => t.title), {
        timeout: 5_000,
        interval: 50,
      })
      .toContain(forged);
    await alice.db
      .insert(app.todos, { title: "still signed in", owner: alice.session!.user! })
      .wait({ tier: "global" });
    expect(alice.db.getSyncStatus().link).toMatchObject({ state: "connected" });
  }, 30_000);

  it("picks up a user's changed claims when the session re-authenticates", async () => {
    // Anyone reads their own todos; only an admin (a claim) may create one.
    const byRole = {
      format: 1,
      tables: {
        todos: { read: "owner = session.user", insert: "session.claims.role = 'admin'" },
      },
    };
    const roles = new Map([["alice", "admin"]]);
    const jazz = await createJazz({
      app,
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
      permissions: testPermissions(app, byRole),
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
});
