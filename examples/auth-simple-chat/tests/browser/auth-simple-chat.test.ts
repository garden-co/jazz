/**
 * Jazz permission-DSL tests for auth-simple-chat.
 *
 * Mints role-tagged JWTs (admin / member / anonymous) against a local
 * JWKS and asserts allow/deny outcomes for posting to ANNOUNCEMENTS_CHAT_ID
 * vs CHAT_ID.
 *
 * NOT covered by `pnpm test`: the runtime auth server's sign-in/sign-up
 * endpoints (server/auth-server.ts), the AuthCard UI, or session
 * restoration across page reload. Those flows live in `pnpm dev` +
 * `pnpm dev:auth` and are exercised by hand.
 */
import { afterEach, describe, expect, it } from "vitest";
import { type JazzClient, createJazzClient } from "jazz-tools/react";
import { createAccountManager, type AccountHandle } from "jazz-tools";
import { app } from "../../schema.js";
import { ANNOUNCEMENTS_CHAT_ID, CHAT_ID } from "../../constants.js";

const clients: JazzClient[] = [];
const accountsByToken = new Map<string, Promise<AccountHandle>>();

afterEach(async () => {
  while (clients.length > 0) {
    await clients.pop()!.shutdown();
  }
});

async function makeClient(jwt?: string): Promise<JazzClient> {
  const manager = await createAccountManager({ appId: __APP_ID__, serverUrl: __JAZZ_SERVER_URL__ });
  let account: AccountHandle;
  if (jwt) {
    let registered = accountsByToken.get(jwt);
    if (!registered) {
      registered = manager.registerJWT({ getToken: async () => jwt });
      accountsByToken.set(jwt, registered);
    }
    account = await registered;
  } else {
    account = manager.createLocalFirst();
  }
  const client = await createJazzClient({
    appId: __APP_ID__,
    serverUrl: __JAZZ_SERVER_URL__,
    driver: { type: "memory" },
    account,
  });
  clients.push(client);
  return client;
}

async function send(client: JazzClient, chat_id: string, text: string): Promise<void> {
  const handle = await client.db.insert(app.messages, {
    author_name: "Tester",
    chat_id,
    text,
    sent_at: new Date(),
  });
  await handle.wait({ tier: "edge" });
}

describe("auth-simple-chat permissions", () => {
  it("admin JWT can post to Announcements and General", async () => {
    const client = await makeClient(__ADMIN_JWT__);
    await expect(send(client, ANNOUNCEMENTS_CHAT_ID, "admin-ann")).resolves.toBeUndefined();
    await expect(send(client, CHAT_ID, "admin-gen")).resolves.toBeUndefined();
  });

  it("member JWT is denied for Announcements but allowed for General", async () => {
    const client = await makeClient(__MEMBER_JWT__);
    await expect(send(client, ANNOUNCEMENTS_CHAT_ID, "member-ann")).rejects.toThrow();
    await expect(send(client, CHAT_ID, "member-gen")).resolves.toBeUndefined();
  });

  it("anonymous (no JWT) is denied for both chats", async () => {
    const client = await makeClient();
    await expect(send(client, ANNOUNCEMENTS_CHAT_ID, "anon-ann")).rejects.toThrow();
    await expect(send(client, CHAT_ID, "anon-gen")).rejects.toThrow();
  });
});
