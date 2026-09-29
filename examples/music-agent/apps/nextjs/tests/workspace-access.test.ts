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
  const token = issuer.jwtForUser("auth-user-1");
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
  accountId = session.account_id!;
  user = await createDb({
    appId: server.appId,
    serverUrl: server.url,
    account,
    driver: { type: "memory" },
  });
});
afterAll(async () => {
  await user?.shutdown();
  await server?.stop();
  await issuer?.stop();
});

describe("MusicAgent workspace access", () => {
  test("a user sees the workspace the server seeded for their account", async () => {
    const { bootstrapWorkspace } = await import("../src/server/bootstrap");
    const firstReply = await bootstrapWorkspace(accountId, "auth-user-1", "Sam");
    expect(firstReply).toBeDefined();

    await expect
      .poll(async () => (await user.all(app.artists, { tier: "global" })).length, {
        timeout: 30_000,
      })
      .toBe(1);
    const conversations = await user.all(app.conversations, { tier: "global" });
    expect(conversations.map((c) => c.title)).toEqual(["Single release show"]);
    const turns = await user.all(app.turns.where({ conversationId: conversations[0]!.id }), {
      tier: "global",
    });
    expect(turns.map((t) => t.role).sort()).toEqual(["assistant", "user"]);
    const files = await user.all(
      app.attachments.where({ conversationId: conversations[0]!.id }).select("filename"),
      { tier: "global" },
    );
    expect(files.map((f) => f.filename)).toEqual(["night-shift-single-rough-mix.wav"]);
    expect(await user.all(app.venues, { tier: "global" })).toHaveLength(8);
  });
});
