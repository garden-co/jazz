import { afterEach, describe, expect, it } from "vitest";
import { type JazzClient, createJazzClient } from "jazz-tools/react";
import { app } from "../../schema.js";
import { ANNOUNCEMENTS_CHAT_ID, CHAT_ID } from "../../constants.js";

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
