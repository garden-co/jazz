/**
 * Jazz permission-DSL tests for auth-simple-chat.
 *
 * Signs test users in through the app's backend with a role claim (admin /
 * member), or as a local-first guest, and asserts allow/deny outcomes for
 * posting to ANNOUNCEMENTS_CHAT_ID vs CHAT_ID.
 *
 * NOT covered by `pnpm test`: the runtime auth server's sign-in/sign-up
 * endpoints (server/auth-server.ts), the AuthCard UI, or session
 * restoration across page reload. Those flows live in `pnpm dev` +
 * `pnpm dev:auth` and are exercised by hand.
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

async function send(client: JazzClient, chat_id: string, text: string): Promise<void> {
  const handle = await client.db.insert(app.messages, {
    author_name: "Tester",
    chat_id,
    text,
    sent_at: new Date(),
  });
  await handle.wait({ tier: "global" });
}

describe("auth-simple-chat permissions", () => {
  it("an admin can post to Announcements and General", async () => {
    const client = await makeClient("admin");
    await expect(send(client, ANNOUNCEMENTS_CHAT_ID, "admin-ann")).resolves.toBeUndefined();
    await expect(send(client, CHAT_ID, "admin-gen")).resolves.toBeUndefined();
  });

  it("a member is denied for Announcements but allowed for General", async () => {
    const client = await makeClient("member");
    await expect(send(client, ANNOUNCEMENTS_CHAT_ID, "member-ann")).rejects.toThrow();
    await expect(send(client, CHAT_ID, "member-gen")).resolves.toBeUndefined();
  });

  it("a guest is denied for both chats", async () => {
    const client = await makeClient();
    await expect(send(client, ANNOUNCEMENTS_CHAT_ID, "anon-ann")).rejects.toThrow();
    await expect(send(client, CHAT_ID, "anon-gen")).rejects.toThrow();
  });
});
