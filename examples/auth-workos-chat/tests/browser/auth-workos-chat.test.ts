/**
 * Jazz permission-DSL tests for auth-workos-chat.
 *
 * Mints WorkOS-shaped JWTs (role under `claims`, ES256) against a local
 * JWKS and asserts allow/deny outcomes for posting to ANNOUNCEMENTS_CHAT_ID
 * vs CHAT_ID.
 *
 * NOT covered by `pnpm test`: the WorkOS OAuth flow, the
 * `@workos-inc/authkit-react` hook, or token refresh. Those are WorkOS's
 * responsibility — what matters here is that a verified JWT with a
 * `role` claim is honoured by `definePermissions`.
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

async function send(client: JazzClient, chat_id: string, text: string): Promise<string> {
  const handle = await client.db.insert(app.messages, {
    author_name: "Tester",
    chat_id,
    text,
    sent_at: new Date(),
  });
  return (await handle.wait({ tier: "edge" })).id;
}

async function update(
  client: JazzClient,
  messageId: string,
  patch: { text?: string; chat_id?: string },
) {
  await client.db.update(app.messages, messageId, patch).wait({ tier: "edge" });
}

async function remove(client: JazzClient, messageId: string): Promise<void> {
  await client.db.delete(app.messages, messageId).wait({ tier: "edge" });
}

describe("auth-workos-chat permissions", () => {
  it("admin JWT can post to Announcements and General", async () => {
    const client = await makeClient(__ADMIN_JWT__);
    await expect(send(client, ANNOUNCEMENTS_CHAT_ID, "admin-ann")).resolves.toEqual(
      expect.any(String),
    );
    await expect(send(client, CHAT_ID, "admin-gen")).resolves.toEqual(expect.any(String));
  });

  it("member JWT is denied for Announcements but allowed for General", async () => {
    const client = await makeClient(__MEMBER_JWT__);
    await expect(send(client, ANNOUNCEMENTS_CHAT_ID, "member-ann")).rejects.toThrow();
    await expect(send(client, CHAT_ID, "member-gen")).resolves.toEqual(expect.any(String));
  });

  it("anonymous (no JWT) is denied for both chats", async () => {
    const client = await makeClient();
    await expect(send(client, ANNOUNCEMENTS_CHAT_ID, "anon-ann")).rejects.toThrow();
    await expect(send(client, CHAT_ID, "anon-gen")).rejects.toThrow();
  });

  it("keeps edits in their original chat while retaining admin and creator authority", async () => {
    const admin = await makeClient(__ADMIN_JWT__);
    const member = await makeClient(__MEMBER_JWT__);

    const announcementId = await send(admin, ANNOUNCEMENTS_CHAT_ID, "admin-ann");
    const adminGenericId = await send(admin, CHAT_ID, "admin-gen");
    const genericId = await send(member, CHAT_ID, "member-gen");

    await expect(
      update(admin, announcementId, { text: "admin-ann-edited" }),
    ).resolves.toBeUndefined();
    await expect(
      update(admin, adminGenericId, { text: "admin-gen-edited" }),
    ).resolves.toBeUndefined();
    await expect(update(member, genericId, { text: "member-gen-edited" })).resolves.toBeUndefined();
    await expect(update(member, adminGenericId, { text: "member-other-edited" })).rejects.toThrow();

    await expect(update(member, genericId, { chat_id: ANNOUNCEMENTS_CHAT_ID })).rejects.toThrow();
    await expect(update(admin, announcementId, { chat_id: CHAT_ID })).rejects.toThrow();
    await expect(
      update(admin, adminGenericId, { chat_id: ANNOUNCEMENTS_CHAT_ID }),
    ).rejects.toThrow();
    await expect(remove(member, genericId)).resolves.toBeUndefined();
    await expect(remove(admin, announcementId)).resolves.toBeUndefined();
  });
});
