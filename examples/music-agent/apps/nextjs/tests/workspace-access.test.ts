import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { accountRegistryUrl, createAccountManager, createDb, type Db } from "jazz-tools";
import { resolveRequestSession } from "jazz-tools/backend";
import {
  startLocalJazzServer,
  startTestJwtIssuer,
  type LocalJazzServerHandle,
  type TestJwtIssuerHandle,
} from "jazz-tools/testing";
import { app } from "../schema";
import permissions from "../permissions";

// The browser's side of the app: a user signed in with an app JWT (as Better
// Auth issues it), reading under the app's permissions what the server seeded
// with backend authority for the account /api/bootstrap resolves.
let issuer: TestJwtIssuerHandle;
let server: LocalJazzServerHandle;
let user: Db;
let accountId: string;
let other: Db;
let otherAccountId: string;

// Signs a Better Auth user in as the browser does and returns their Jazz
// client and the account id the server resolves for their token.
async function signIn(sub: string) {
  let stored: string | null = null;
  const accounts = await createAccountManager({
    appId: server.appId,
    serverUrl: server.url,
    store: {
      read: async () => stored,
      update: async (transform) => {
        stored = transform(stored);
      },
    },
  });
  const token = issuer.jwtForUser(sub);
  const account = await accounts.registerJWT({ getToken: async () => token });
  const session = await resolveRequestSession(
    new Request("http://app.test/api/bootstrap", { headers: { authorization: `Bearer ${token}` } }),
    {
      appId: server.appId,
      accountRegistry: accountRegistryUrl(server.url, server.appId),
      jwksUrl: issuer.jwksUrl,
      jwtIssuer: issuer.issuer,
      jwtAudience: issuer.audience,
    },
  );
  expect(session.account_id).toBe(account.id);
  const db = await createDb({
    appId: server.appId,
    serverUrl: server.url,
    account,
    driver: { type: "memory" },
  });
  return { db, accountId: session.account_id! };
}
beforeAll(async () => {
  issuer = await startTestJwtIssuer();
  server = await startLocalJazzServer({
    inMemory: true,
    schema: app,
    permissions,
    jwksUrl: issuer.jwksUrl,
    jwtIssuer: issuer.issuer,
    jwtAudience: issuer.audience,
  });
  process.env.NEXT_PUBLIC_JAZZ_SERVER_URL = server.url;
  process.env.NEXT_PUBLIC_JAZZ_APP_ID = server.appId;
  process.env.BACKEND_SECRET = server.backendSecret;
  process.env.BETTER_AUTH_SECRET = "music-agent-test-better-auth-secret-0123456789";
  process.env.MUSIC_AGENT_PROVIDER = "scripted";
  process.env.SCRIPTED_AGENT_TOKEN_DELAY_MS = "0";

  ({ db: user, accountId } = await signIn("auth-user-1"));
  ({ db: other, accountId: otherAccountId } = await signIn("auth-user-2"));
});
afterAll(async () => {
  await user?.shutdown();
  await other?.shutdown();
  await server?.stop();
  await issuer?.stop();
});

describe("MusicAgent workspace access", () => {
  test("a user sees the workspace the server seeded for their account", async () => {
    const { bootstrapWorkspace } = await import("../src/server/bootstrap");
    const firstReply = await bootstrapWorkspace(accountId, "auth-user-1", "Sam");
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
    await bootstrapWorkspace(otherAccountId, "auth-user-2", "Alex");
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
