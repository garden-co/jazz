/**
 * Jazz permission-DSL tests for auth-betterauth-chat.
 *
 * Mints role-tagged JWTs against a local JWKS and asserts that only an
 * `admin` may insert, update, or delete Announcements while generic-chat
 * updates and deletes remain bound to the message creator.
 *
 * NOT covered by `pnpm test`: Better Auth's sign-up / sign-in API
 * (`src/lib/auth.ts`), the JWKS endpoint at `/api/auth/jwks`, the
 * AuthCard UI, or session cookies. Those run in `pnpm dev` against the
 * actual Better Auth server and aren't exercised here.
 */
import { afterEach, describe, expect, it } from "vitest";
import { type JazzClient, createJazzClient } from "jazz-tools/react";
import { app } from "../../schema.js";
import permissions from "../../permissions.js";
import { schema as betterAuthSchema } from "../../schema-better-auth/schema.js";

const clients: JazzClient[] = [];

afterEach(async () => {
  while (clients.length > 0) {
    await clients.pop()!.shutdown();
  }
});

async function makeClient(jwt?: string): Promise<JazzClient> {
  const client = await createJazzClient({
    appId: __APP_ID__,
    serverUrl: __JAZZ_SERVER_URL__,
    driver: { type: "memory" },
    ...(jwt ? { jwtToken: jwt } : {}),
  });
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
  const message = await handle.wait({ tier: "edge" });
  return message.id;
}

async function update(client: JazzClient, messageId: string, text: string): Promise<void> {
  await client.db.update(app.messages, messageId, { text }).wait({ tier: "edge" });
}

async function move(client: JazzClient, messageId: string, chat_id: string): Promise<void> {
  await client.db.update(app.messages, messageId, { chat_id }).wait({ tier: "edge" });
}

async function remove(client: JazzClient, messageId: string): Promise<void> {
  await client.db.delete(app.messages, messageId).wait({ tier: "edge" });
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
    const admin = await makeClient(__ADMIN_JWT__);
    const member = await makeClient(__MEMBER_JWT__);

    await expect(send(member, __ANNOUNCEMENTS_CHAT_ID__, "member-ann")).rejects.toThrow();

    const messageId = await send(admin, __ANNOUNCEMENTS_CHAT_ID__, "admin-ann");
    await expect(update(member, messageId, "member edit")).rejects.toThrow();
    await expect(update(admin, messageId, "admin edit")).resolves.toBeUndefined();
    await expect(remove(member, messageId)).rejects.toThrow();
    await expect(remove(admin, messageId)).resolves.toBeUndefined();
  });

  it("keeps generic-chat updates and deletes bound to the creator", async () => {
    const admin = await makeClient(__ADMIN_JWT__);
    const member = await makeClient(__MEMBER_JWT__);

    const messageId = await send(member, __CHAT_ID__, "member-gen");
    await expect(update(member, messageId, "member edit")).resolves.toBeUndefined();
    await expect(update(admin, messageId, "admin edit")).rejects.toThrow();
    await expect(remove(admin, messageId)).rejects.toThrow();
    await expect(move(member, messageId, __ANNOUNCEMENTS_CHAT_ID__)).rejects.toThrow();
    await expect(remove(member, messageId)).resolves.toBeUndefined();
  });

  it("anonymous can post to General but is denied for Announcements", async () => {
    const client = await makeClient();
    await expect(send(client, __ANNOUNCEMENTS_CHAT_ID__, "anon-ann")).rejects.toThrow();
    await expect(send(client, __CHAT_ID__, "anon-gen")).resolves.toEqual(expect.any(String));
  });
});
