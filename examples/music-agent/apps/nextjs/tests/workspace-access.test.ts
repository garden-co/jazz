import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import type { Db } from "jazz-tools";
import { startLocalJazzServer, type LocalJazzServerHandle } from "jazz-tools/testing";
import { app } from "../schema";
import permissions from "../permissions";
import permissionsFile from "../jazz.permissions.json" with { type: "json" };

// The browser's side of the app: a user signed in with Better Auth, syncing
// through the app's own `/jazz` route (as the browser does), and reading under
// the app's permissions what the server seeded with backend authority for the
// account the app's Jazz resolves for them.
let server: LocalJazzServerHandle;
const httpServers: Server[] = [];
const closers: Array<() => Promise<void>> = [];
let user: Db;
let accountId: string;
let other: Db;
let otherAccountId: string;

// Each signed-in browser reaches the app on its own origin, so the cookie of
// the right user rides every request and socket to it, as it would in a tab.
const cookies = new Map<string, string>();
const realFetch = globalThis.fetch;
const RealWebSocket = globalThis.WebSocket;
function installBrowserCookies() {
  globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    const request = new Request(input, init);
    const cookie = cookies.get(new URL(request.url).host);
    if (cookie) request.headers.set("cookie", cookie);
    return realFetch(request);
  }) as typeof fetch;
  globalThis.WebSocket = class extends RealWebSocket {
    constructor(url: string | URL, options?: { headers?: Record<string, string> }) {
      const cookie = cookies.get(new URL(url).host);
      super(url, {
        ...options,
        headers: { ...options?.headers, ...(cookie ? { cookie } : {}) },
      } as never);
    }
  } as typeof WebSocket;
}

// The app's Next route, served the way `app/jazz/[...path]/route.ts` mounts it.
async function serveApp(): Promise<string> {
  const { backendJazzClient } = await import("../src/lib/backend-jazz-client");
  const jazz = await backendJazzClient();
  const http = createServer((req, res) =>
    jazz.nodeHandler(req, res, () => {
      res.statusCode = 404;
      res.end();
    }),
  );
  await jazz.attach(http);
  await new Promise<void>((resolve) => http.listen(0, "127.0.0.1", resolve));
  httpServers.push(http);
  return `http://127.0.0.1:${(http.address() as AddressInfo).port}`;
}

// Signs a user up with Better Auth as the browser does and returns their Jazz
// client and the account the app's Jazz resolves for their session.
async function signIn(email: string, name: string) {
  const { auth } = await import("../src/lib/auth");
  const signUp = await auth.api.signUpEmail({
    body: { email, password: "correct horse battery", name },
    asResponse: true,
  });
  expect(signUp.ok).toBe(true);
  const cookie = signUp.headers
    .getSetCookie()
    .map((line) => line.split(";")[0])
    .join("; ");
  const origin = await serveApp();
  cookies.set(new URL(origin).host, cookie);

  const { backendJazzClient } = await import("../src/lib/backend-jazz-client");
  const jazz = await backendJazzClient();
  const session = await auth.api.getSession({ headers: new Headers({ cookie }) });
  // The browser's sign-in gives the user their Jazz account; the app's Jazz
  // then resolves the same account for their session.
  const { createJazzClient } = await import("jazz-tools/backend");
  const browser = await createJazzClient({
    appId: server.appId,
    app,
    permissions,
    serverUrl: `${origin}/jazz`,
    driver: { type: "memory" },
  });
  closers.push(() => browser.close());
  await browser.refresh();
  const { user, status, error } = browser.getSnapshot();
  if (status !== "ready") throw error ?? new Error(status);
  const account = await jazz.accountFor(
    new Request(`${origin}/api/bootstrap`, { headers: { cookie } }),
  );
  if (!account) throw new Error(`the app's Jazz resolves no account for ${email}`);
  expect(user?.accountId).toBe(account);
  return { db: browser.db, accountId: account, userId: session!.user.id };
}
let userId: string;
let otherUserId: string;
beforeAll(async () => {
  server = await startLocalJazzServer({ inMemory: true, schema: app, permissions: permissionsFile });
  // The app's Jazz fronts this test server with remote().
  process.env.JAZZ_SERVER_URL = server.url;
  process.env.JAZZ_ADMIN_SECRET = server.adminSecret;
  process.env.JAZZ_APP_ID = server.appId;
  process.env.BACKEND_SECRET = server.backendSecret;
  process.env.BETTER_AUTH_SECRET = "music-agent-test-better-auth-secret-0123456789";
  process.env.MUSIC_AGENT_PROVIDER = "scripted";
  process.env.SCRIPTED_AGENT_TOKEN_DELAY_MS = "0";
  installBrowserCookies();

  ({ db: user, accountId, userId } = await signIn("sam@example.test", "Sam"));
  ({
    db: other,
    accountId: otherAccountId,
    userId: otherUserId,
  } = await signIn("alex@example.test", "Alex"));
});
afterAll(async () => {
  for (const close of closers) await close();
  for (const http of httpServers) {
    http.closeAllConnections();
    await new Promise((resolve) => http.close(resolve));
  }
  globalThis.fetch = realFetch;
  globalThis.WebSocket = RealWebSocket;
  await (await (await import("../src/lib/backend-jazz-client")).backendJazzClient()).close();
  await server?.stop();
});

describe("MusicAgent workspace access", () => {
  test("a user sees the workspace the server seeded for their account", async () => {
    const { bootstrapWorkspace } = await import("../src/server/bootstrap");
    const firstReply = await bootstrapWorkspace(accountId, userId, "Sam");
    expect(firstReply).toBeDefined();

    await expect
      .poll(async () => (await user.all(app.artists, { tier: "remote" })).length, {
        timeout: 30_000,
      })
      .toBe(1);
    const conversations = await user.all(app.conversations, { tier: "remote" });
    expect(conversations.map((c) => c.title)).toEqual(["Single release show"]);
    const turns = await user.all(app.turns.where({ conversationId: conversations[0]!.id }), {
      tier: "remote",
    });
    expect(turns.map((t) => t.role).sort()).toEqual(["assistant", "user"]);
    const files = await user.all(
      app.attachments.where({ conversationId: conversations[0]!.id }).select("filename"),
      { tier: "remote" },
    );
    expect(files.map((f) => f.filename)).toEqual(["night-shift-single-rough-mix.wav"]);
    expect(await user.all(app.venues, { tier: "remote" })).toHaveLength(8);
  });

  test("a second user sees none of it and cannot build on it", async () => {
    const { bootstrapWorkspace } = await import("../src/server/bootstrap");
    await bootstrapWorkspace(otherAccountId, otherUserId, "Alex");
    await expect
      .poll(async () => (await other.all(app.artists, { tier: "remote" })).length, {
        timeout: 30_000,
      })
      .toBe(1);

    const [ownArtist] = await other.all(app.artists, { tier: "remote" });
    const [ownConversation] = await other.all(app.conversations, { tier: "remote" });
    expect(ownArtist!.ownerAccount).toBe(otherAccountId);
    expect(ownConversation!.ownerAccount).toBe(otherAccountId);

    // The first user's rows stay invisible to the second.
    const [samArtist] = await user.all(app.artists, { tier: "remote" });
    const [samConversation] = await user.all(app.conversations, { tier: "remote" });
    const [samTurn] = await user.all(app.turns.where({ conversationId: samConversation!.id }), {
      tier: "remote",
    });
    expect(
      await other.one(app.artists.where({ id: samArtist!.id }), { tier: "remote" }),
    ).toBeNull();
    expect(
      await other.all(app.turns.where({ conversationId: samConversation!.id }), { tier: "remote" }),
    ).toEqual([]);

    // A conversation of their own that names the first user's artist is refused.
    await expect(
      other
        .insert(app.conversations, {
          ownerAccount: otherAccountId,
          artistId: samArtist!.id,
          title: "Borrowed artist",
        })
        .wait({ tier: "global" }),
    ).rejects.toMatchObject({ code: "permission_denied" });

    // A turn continuing their own conversation is fine; one that continues the
    // first user's is refused.
    const [ownTurn] = await other.all(app.turns.where({ conversationId: ownConversation!.id }), {
      tier: "remote",
    });
    await other
      .insert(app.turns, {
        conversationId: ownConversation!.id,
        parentId: ownTurn!.id,
        role: "user",
        body: "Any venues in Milwaukee?",
        status: "complete",
      })
      .wait({ tier: "global" });
    await expect(
      other
        .insert(app.turns, {
          conversationId: ownConversation!.id,
          parentId: samTurn!.id,
          role: "user",
          body: "Carry on from Sam's plan",
          status: "complete",
        })
        .wait({ tier: "global" }),
    ).rejects.toMatchObject({ code: "permission_denied" });
  });
});
