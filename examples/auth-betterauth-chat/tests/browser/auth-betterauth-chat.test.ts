/**
 * Jazz permission-DSL tests for auth-betterauth-chat.
 *
 * Mints a verified JWT against a local JWKS and asserts that the
 * `authMode: "external"` permission rule lets authenticated callers
 * post to either chat while denying anonymous callers for Announcements.
 *
 * NOT covered by `pnpm test`: Better Auth's sign-up / sign-in API
 * (`src/lib/auth.ts`), the JWKS endpoint at `/api/auth/jwks`, the
 * AuthCard UI, or session cookies. Those run in `pnpm dev` against the
 * actual Better Auth server and aren't exercised here.
 */
import { afterEach, describe, expect, it } from "vitest";
import { type JazzClient, createJazzClient } from "jazz-tools/react";
import { app } from "../../schema.js";

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

async function send(client: JazzClient, chat_id: string, text: string): Promise<void> {
  const handle = client.db.insert(app.messages, {
    author_name: "Tester",
    chat_id,
    text,
    sent_at: new Date(),
  });
  await handle.wait({ tier: "edge" });
}

describe("auth-betterauth-chat permissions", () => {
  it("authenticated JWT can post to Announcements and General", async () => {
    const client = await makeClient(__USER_JWT__);
    await expect(send(client, __ANNOUNCEMENTS_CHAT_ID__, "user-ann")).resolves.toBeUndefined();
    await expect(send(client, __CHAT_ID__, "user-gen")).resolves.toBeUndefined();
  });

  it("anonymous can post to General but is denied for Announcements", async () => {
    const client = await makeClient();
    await expect(send(client, __ANNOUNCEMENTS_CHAT_ID__, "anon-ann")).rejects.toThrow();
    await expect(send(client, __CHAT_ID__, "anon-gen")).resolves.toBeUndefined();
  });
});
