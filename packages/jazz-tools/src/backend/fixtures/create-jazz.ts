/**
 * Shared setup for the createJazz integration tests, split across
 * files so their slow cases run in parallel workers instead of one after another.
 */
import { randomBytes } from "node:crypto";
import { createServer, request as httpRequest, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { type Http2SecureServer, type Http2Server } from "node:http2";
import { afterEach } from "vitest";
import { testApp, testPermissions } from "../../testing/index.js";
import { acceptDeviceAccounts, createJazzClient, type Jazz } from "../index.js";

export const app = testApp({
  tables: { todos: { columns: { title: "string", owner: "uuid" } } },
});
export const permissions = {
  format: 1,
  tables: { todos: { read: "owner = session.user", insert: "owner = session.user" } },
};
/** The same permissions, compiled, for a browser session (`createJazzClient`). */
export const clientPermissions = testPermissions(app, permissions);

export const NodeWebSocket = globalThis.WebSocket;
export const cleanups: Array<() => Promise<unknown>> = [];
/** Runs the cleanups each test registered; call it once per test file. */
export function runCleanupsAfterEach() {
  afterEach(async () => {
    while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
  });
}

/** The test app's own auth: the user named in a header, else a local-first guest. */
export function authenticate(request: Request) {
  const userId = request.headers.get("x-test-user");
  return userId ? { userId, expiresAt: inAnHour() } : acceptDeviceAccounts(request);
}

/** An `expiresAt` an hour from now, in Unix seconds. */
export function inAnHour() {
  return Math.floor(Date.now() / 1000) + 3600;
}

/** An `expiresAt` `seconds` from now, in Unix seconds (fractional is fine). */
export function inSeconds(seconds: number) {
  return Date.now() / 1000 + seconds;
}

/** Sample `read()` every 10 ms while `during` runs; resolves with every distinct value seen. */
export async function watchWhile<T>(read: () => T, during: Promise<unknown>): Promise<T[]> {
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

export const requestPaths: string[] = [];
/** The stream key each request carried, by request path. */
export const streamKeys = new Map<string, string>();

export async function serve(
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
export function statusOf(
  url: string,
  method: string,
  headers: Record<string, string>,
): Promise<number> {
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
export function signInAs(origin: string, userId: string) {
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

export async function userSession(appId: string, origin: string, userId: string) {
  signInAs(origin, userId);
  const session = await createJazzClient({
    appId,
    app,
    permissions: clientPermissions,
    serverUrl: `${origin}/jazz`,
    driver: { type: "memory" },
  });
  cleanups.push(() => session.close());
  await session.refresh();
  const { client, status, error } = session.getSnapshot();
  if (status !== "ready") throw error ?? new Error(status);
  return client!;
}

export const randomBytesBase64 = () => randomBytes(16).toString("base64");

// Hono's `serve()` returns any of these, and `attach()` takes each.
export type AttachesHonoServers = [Server | Http2Server | Http2SecureServer] extends [
  Parameters<Jazz["attach"]>[0],
]
  ? true
  : never;
export const attachesHonoServers: AttachesHonoServers = true;
void attachesHonoServers;
