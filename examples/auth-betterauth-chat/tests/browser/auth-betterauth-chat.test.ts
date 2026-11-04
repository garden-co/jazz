/**
 * Jazz permission-DSL tests for auth-betterauth-chat.
 *
 * Signs test users in through the app's backend with a role claim and asserts
 * that only an `admin` may insert, update, or delete Announcements while
 * generic-chat updates and deletes remain bound to the message creator.
 *
 * The server suite separately exercises the actual Better Auth handler,
 * session cookies and sign-in; these browser tests exercise Jazz policies.
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
import { schema as betterAuthSchema } from "../../schema-better-auth/schema.js";

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
  while (clients.length > 0) await clients.pop()!.shutdown();
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
  const handle = client.db.insert(app.messages, {
    author_name: "Tester",
    chat_id,
    text,
    sent_at: new Date(),
  });
  const message = await handle.wait({ tier: "global" });
  return message.id;
}

async function update(client: JazzClient, messageId: string, text: string): Promise<void> {
  await client.db.update(app.messages, messageId, { text }).wait({ tier: "global" });
}

async function move(client: JazzClient, messageId: string, chat_id: string): Promise<void> {
  await client.db.update(app.messages, messageId, { chat_id }).wait({ tier: "global" });
}

async function remove(client: JazzClient, messageId: string): Promise<void> {
  await client.db.delete(app.messages, messageId).wait({ tier: "global" });
}

describe("auth-betterauth-chat permissions", () => {
  it("composes deny-all CRUD policies for every generated Better Auth table", () => {
    for (const tableName of Object.keys(betterAuthSchema)) {
      const tablePermissions = permissions[tableName]!;

      expect(tablePermissions.select?.using).toEqual({ type: "False" });
      expect(tablePermissions.insert?.with_check).toEqual({ type: "False" });
      expect(tablePermissions.update?.using).toEqual({ type: "False" });
      expect(tablePermissions.update?.with_check).toEqual({ type: "False" });
      expect(tablePermissions.delete?.using).toEqual({ type: "False" });
    }
  });

  it("uses one update predicate for both the old and new message", () => {
    // Update `using` evaluates the old row and `with_check` evaluates the new
    // one. They must use the same room/role predicate; independently OR-ing
    // per-room rules lets a General-message creator switch its chat_id to
    // Announcements.
    expect(permissions.messages!.update?.using).toEqual(permissions.messages!.update?.with_check);
  });

  it("allows only admins to mutate Announcements", async () => {
    const admin = await makeClient("admin");
    const member = await makeClient("member");

    await expect(send(member, __ANNOUNCEMENTS_CHAT_ID__, "member-ann")).rejects.toThrow();

    const messageId = await send(admin, __ANNOUNCEMENTS_CHAT_ID__, "admin-ann");
    await expect(update(member, messageId, "member edit")).rejects.toThrow();
    await expect(update(admin, messageId, "admin edit")).resolves.toBeUndefined();
    await expect(remove(member, messageId)).rejects.toThrow();
    await expect(remove(admin, messageId)).resolves.toBeUndefined();
  });

  it("keeps generic-chat updates and deletes bound to the creator", async () => {
    const admin = await makeClient("admin");
    const member = await makeClient("member");

    const messageId = await send(member, __CHAT_ID__, "member-gen");
    await expect(update(member, messageId, "member edit")).resolves.toBeUndefined();
    await expect(update(admin, messageId, "admin edit")).rejects.toThrow();
    await expect(remove(admin, messageId)).rejects.toThrow();
    await expect(move(member, messageId, __ANNOUNCEMENTS_CHAT_ID__)).rejects.toThrow();
    await expect(remove(member, messageId)).resolves.toBeUndefined();
  });

  it("a guest can post to General but is denied for Announcements", async () => {
    const client = await makeClient();
    await expect(send(client, __ANNOUNCEMENTS_CHAT_ID__, "anon-ann")).rejects.toThrow();
    await expect(send(client, __CHAT_ID__, "anon-gen")).resolves.toEqual(expect.any(String));
  });
});
