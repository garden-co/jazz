import { describe, expect, it } from "vitest";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { decodeWebSocketFrameBatch } from "../runtime/native-runtime/websocket.js";
import { createJazz, memory } from "./index.js";
import {
  app,
  authenticate,
  cleanups,
  inSeconds,
  permissions,
  requestPaths,
  runCleanupsAfterEach,
  serve,
  statusOf,
  userSession,
  watchWhile,
} from "./fixtures/create-jazz.js";

runCleanupsAfterEach();

describe("createJazz: renewing sessions in place", () => {
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
});
