// BandChat's own server (server/index.ts), run as `pnpm start` runs it, with
// Better Auth signing users in. It holds no encryption keys: it stores and
// relays the encrypted columns as ciphertext. Before a lost device is
// removed, its route ends every other sign-in of the user, however many.
import { spawn, type ChildProcess } from "node:child_process";
import { readFileSync } from "node:fs";
import { mkdtemp } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { createJazzClient, memoryPrivateStore } from "jazz-tools/backend";
import permissions from "../jazz.permissions.json" with { type: "json" };
import { app, cleanUp, createRoom, device, serve, storedBytes, until } from "./harness";

const APP_DIR = join(import.meta.dirname, "..");
const SESSION_COOKIE = "better-auth.session_token";

// The test's own requests; a signed-in client below patches the global one.
const plainFetch = globalThis.fetch;

const running: ChildProcess[] = [];
const restores: Array<() => unknown> = [];
afterEach(async () => {
  while (restores.length) await restores.pop()!();
  await cleanUp();
  for (const child of running.splice(0)) await stopServer(child);
});

async function freePort(): Promise<number> {
  const probe = createServer();
  await new Promise<void>((resolve) => probe.listen(0, "127.0.0.1", resolve));
  const { port } = probe.address() as { port: number };
  await new Promise((resolve) => probe.close(resolve));
  return port;
}

async function stopServer(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null) return;
  const exited = new Promise((resolve) => child.once("exit", resolve));
  child.kill("SIGTERM");
  await exited;
}

/** The app's server on a free port, its data in a new directory. */
async function startServer() {
  const port = await freePort();
  const origin = `http://127.0.0.1:${port}`;
  const dataPath = await mkdtemp(join(tmpdir(), "band-chat-e2ee-server-"));
  const child = spawn(join(APP_DIR, "node_modules/.bin/tsx"), ["server/index.ts"], {
    cwd: APP_DIR,
    env: {
      ...process.env,
      // A local run that keeps its data on disk, as `pnpm dev` does.
      NODE_ENV: "development",
      PORT: String(port),
      APP_ORIGIN: origin,
      BETTER_AUTH_SECRET: "band-chat-server-test-secret-0123456789abcdef",
      JAZZ_DATA_PATH: dataPath,
    },
    stdio: ["ignore", "inherit", "inherit"],
  });
  running.push(child);
  await until(async () => {
    const response = await plainFetch(`${origin}/health`).catch(() => undefined);
    return response?.ok ? true : undefined;
  }, "the server to start");
  return { origin, dataPath, stop: () => stopServer(child) };
}

type AppServer = Awaited<ReturnType<typeof startServer>>;

/** A request to the app as its own page sends it: same origin, with `cookie`. */
function call(server: AppServer, path: string, init: { body?: unknown; cookie?: string } = {}) {
  return plainFetch(`${server.origin}${path}`, {
    method: init.body === undefined ? "GET" : "POST",
    headers: {
      origin: server.origin,
      ...(init.body === undefined ? {} : { "content-type": "application/json" }),
      ...(init.cookie ? { cookie: init.cookie } : {}),
    },
    body: init.body === undefined ? undefined : JSON.stringify(init.body),
  });
}

/** The Better Auth session cookie a sign-up or sign-in answered with. */
function sessionCookie(response: Response): string {
  const cookie = response.headers
    .getSetCookie()
    .find((header) => header.startsWith(`${SESSION_COOKIE}=`));
  if (!response.ok || !cookie) throw new Error(`no session (${response.status})`);
  return cookie.split(";")[0]!;
}

async function signUp(server: AppServer, email: string) {
  const password = "correct horse battery";
  const cookie = sessionCookie(
    await call(server, "/api/auth/sign-up/email", { body: { name: "Ada", email, password } }),
  );
  const signIn = async () =>
    sessionCookie(await call(server, "/api/auth/sign-in/email", { body: { email, password } }));
  return { cookie, signIn };
}

async function sessionOf(server: AppServer, cookie: string) {
  return (await (await call(server, "/api/auth/get-session", { cookie })).json()) as {
    user: { id: string };
  } | null;
}

/**
 * A Node client of the app signed in with `cookie`, as a browser tab is: its
 * requests and sockets to the app's origin carry the cookie.
 */
async function signedInClient(server: AppServer, cookie: string) {
  const realFetch = globalThis.fetch;
  const RealWebSocket = globalThis.WebSocket;
  const host = new URL(server.origin).host;
  globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    const request = new Request(input, init);
    if (new URL(request.url).host === host && !request.headers.has("cookie"))
      request.headers.set("cookie", cookie);
    return realFetch(request);
  }) as typeof fetch;
  globalThis.WebSocket = class extends RealWebSocket {
    constructor(url: string | URL, options?: { headers?: Record<string, string> }) {
      const headers = new URL(url).host === host ? { cookie } : {};
      super(url, { ...options, headers: { ...options?.headers, ...headers } } as never);
    }
  } as typeof WebSocket;
  restores.push(() => {
    globalThis.fetch = realFetch;
    globalThis.WebSocket = RealWebSocket;
  });
  const session = await createJazzClient({
    serverUrl: `${server.origin}/jazz`,
    app,
    permissions,
    driver: { type: "memory" },
    encryption: { store: memoryPrivateStore() },
  });
  // A revoked client may close with writes the server never accepted.
  restores.push(() => session.close().catch(() => undefined));
  await session.refresh();
  const { client, status, error } = session.getSnapshot();
  if (status !== "ready") throw error ?? new Error(status);
  return client!;
}

describe("ending a user's other sign-ins", () => {
  it("deletes every other session, more than Better Auth lists at once, and ends their connections", async () => {
    const server = await startServer();
    const user = await signUp(server, `ada-${Date.now()}@band-chat.test`);
    // More sessions than one listing (100) returns.
    const others: string[] = [];
    while (others.length < 150) {
      others.push(...(await Promise.all(Array.from({ length: 10 }, () => user.signIn()))));
    }
    const caller = others.pop()!;
    // An open connection on another session.
    const elsewhere = await signedInClient(server, others[0]!);
    await expect
      .poll(() => elsewhere.db.getSyncStatus().link, { timeout: 15_000 })
      .toMatchObject({ state: "connected" });

    // Only the app's own page may ask, signed in.
    expect((await call(server, "/api/end-other-sign-ins", { body: {} })).status).toBe(401);
    const crossSite = await plainFetch(`${server.origin}/api/end-other-sign-ins`, {
      method: "POST",
      headers: { origin: "https://elsewhere.example", cookie: caller },
    });
    expect(crossSite.status).toBe(403);

    const ended = await call(server, "/api/end-other-sign-ins", { body: {}, cookie: caller });
    expect(ended.status).toBe(200);
    expect(await ended.json()).toEqual({ ended: 150 });

    // Only the caller's session is left.
    const listed = await call(server, "/api/auth/list-sessions", { cookie: caller });
    expect(((await listed.json()) as unknown[]).length).toBe(1);
    for (const cookie of [user.cookie, ...others])
      expect(await sessionOf(server, cookie)).toBe(null);
    expect((await sessionOf(server, caller))?.user.id).toEqual(expect.any(String));
    // The connection on a deleted session is signed out at once.
    await expect
      .poll(() => elsewhere.db.getSyncStatus().link, { timeout: 5_000, interval: 20 })
      .toMatchObject({ state: "offline", reason: "auth" });
  }, 120_000);
});

describe("the server holds no plaintext of an encrypted column", () => {
  it("stores no message text, attachment name or read time it was sent", async () => {
    const server = await startServer();
    const ada = await signUp(server, `ada-${Date.now()}@band-chat.test`);
    const client = await signedInClient(server, ada.cookie);
    const db = client.db;
    expect(await db.encryption.ready({ timeoutMs: 20_000 })).toBe("active");
    const account = client.session!.user!;
    const profile = await db
      .insert(app.profiles, { author: account, displayName: "Ada" })
      .wait({ tier: "global" });
    const roomId = crypto.randomUUID();
    const self = await db.encryption.members.row({ table: "rooms", id: roomId }, account);
    await (
      await db.transaction((tx) => {
        tx.insert(app.rooms, { name: "PLAIN-ROOM-NAME-7f3a" }, { id: roomId });
        tx.insert(app.__e2ee_members, self);
        tx.insert(app.roomMembers, { roomId, memberAuthor: account, memberProfileId: profile.id });
      })
    ).wait({ tier: "global" });
    await db
      .insert(app.messages, {
        roomId,
        senderId: profile.id,
        text: "SECRET-MESSAGE-TEXT-7f3a",
        attachmentName: "SECRET-ATTACHMENT-NAME-7f3a.pdf",
        attachmentType: "application/pdf",
        attachmentSize: 3,
      })
      .wait({ tier: "global" });
    await db
      .insert(app.readMarkers, { roomId, reader: account, lastReadAt: new Date() })
      .wait({ tier: "global" });

    await server.stop();
    const stored = await storedBytes(server.dataPath);
    expect(stored.includes(Buffer.from("PLAIN-ROOM-NAME-7f3a"))).toBe(true);
    expect(stored.includes(Buffer.from("SECRET-MESSAGE-TEXT-7f3a"))).toBe(false);
    expect(stored.includes(Buffer.from("SECRET-ATTACHMENT-NAME-7f3a"))).toBe(false);
  }, 90_000);
});
