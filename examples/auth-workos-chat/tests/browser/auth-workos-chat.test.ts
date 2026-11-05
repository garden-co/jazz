/**
 * Jazz permission-DSL tests for auth-workos-chat.
 *
 * Signs test users in through the app's backend with the `role` claim its
 * `authenticate` reads from a WorkOS session, or as a local-first guest, and
 * asserts allow/deny outcomes for posting to ANNOUNCEMENTS_CHAT_ID vs CHAT_ID.
 *
 * NOT covered by `pnpm test`: the WorkOS OAuth flow, the
 * `@workos-inc/authkit-react` hook, or token refresh. Those are WorkOS's
 * responsibility — what matters here is that a signed-in session with a
 * `role` claim is honoured by `definePermissions`.
 */
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import type { Db } from "../../../../packages/jazz-tools/src/runtime/db.js";
import {
  acquireBrowserTestAccount,
  createBrowserTestDb,
} from "../../../../packages/jazz-tools/tests/browser/account-fixtures.js";
import { deploy } from "../../../../packages/jazz-tools/src/dev/catalogue.js";
import {
  getJazzServerInfo,
  jazzAppUserUrl,
  loginAtJazzAppUser,
  startJazzApp,
  type JazzServerInfo,
} from "../../../../packages/jazz-tools/tests/browser/testing-server.js";
import { app } from "../../schema.js";
import permissions from "../../permissions.js";
import { ANNOUNCEMENTS_CHAT_ID, CHAT_ID } from "../../constants.js";

/** A test user's client: its Db, and how to close it. */
type JazzClient = { db: Db; shutdown(): Promise<void> };

const clients: JazzClient[] = [];
const userUrls = new Map<Role, Promise<string>>();
let server: JazzServerInfo;

type Role = "admin" | "member";

// The app's backend (createJazz) in front of a test core, as the app runs
// it. Its `authenticate` names each test user with their role claim.
beforeAll(async () => {
  const core = await getJazzServerInfo(__APP_ID__);
  await deploy({ ...core, schema: app, permissions });
  server = await startJazzApp(core, { schema: app, permissions });
});

afterEach(async () => {
  while (clients.length > 0) {
    await clients.pop()!.shutdown();
  }
});

/** A client signed in with `role`, or a local-first guest without one. */
async function makeClient(role?: Role): Promise<JazzClient> {
  let serverUrl = server.serverUrl;
  let account;
  if (role) {
    // A user signs in through the app's backend on their own origin; each
    // sign-in there names the same user, so the same account.
    let userUrl = userUrls.get(role);
    if (!userUrl) {
      userUrl = jazzAppUserUrl(server, `test-${role}`, { role });
      userUrls.set(role, userUrl);
    }
    serverUrl = await userUrl;
    account = await loginAtJazzAppUser(server.appId, serverUrl);
  } else {
    account = await acquireBrowserTestAccount({
      appId: server.appId,
      serverUrl,
      key: crypto.randomUUID(),
    });
  }
  const db = await createBrowserTestDb({
    appId: server.appId,
    serverUrl,
    driver: { type: "memory" },
    account,
  });
  const client = { db, shutdown: () => db.shutdown() };
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
  return (await handle.wait({ tier: "global" })).id;
}

async function update(
  client: JazzClient,
  messageId: string,
  patch: { text?: string; chat_id?: string },
) {
  await client.db.update(app.messages, messageId, patch).wait({ tier: "global" });
}

async function remove(client: JazzClient, messageId: string): Promise<void> {
  await client.db.delete(app.messages, messageId).wait({ tier: "global" });
}

describe("auth-workos-chat permissions", () => {
  it("an admin can post to Announcements and General", async () => {
    const client = await makeClient("admin");
    await expect(send(client, ANNOUNCEMENTS_CHAT_ID, "admin-ann")).resolves.toEqual(
      expect.any(String),
    );
    await expect(send(client, CHAT_ID, "admin-gen")).resolves.toEqual(expect.any(String));
  });

  it("a member is denied for Announcements but allowed for General", async () => {
    const client = await makeClient("member");
    await expect(send(client, ANNOUNCEMENTS_CHAT_ID, "member-ann")).rejects.toThrow();
    await expect(send(client, CHAT_ID, "member-gen")).resolves.toEqual(expect.any(String));
  });

  it("a guest is denied for both chats", async () => {
    const client = await makeClient();
    await expect(send(client, ANNOUNCEMENTS_CHAT_ID, "anon-ann")).rejects.toThrow();
    await expect(send(client, CHAT_ID, "anon-gen")).rejects.toThrow();
  });

  it("keeps edits in their original chat while retaining admin and creator authority", async () => {
    const admin = await makeClient("admin");
    const member = await makeClient("member");

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
