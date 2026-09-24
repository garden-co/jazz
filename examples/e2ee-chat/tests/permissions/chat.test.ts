import { expect, it } from "vitest";
import { createAccountManager, createDb, type AccountStore, type Db } from "jazz-tools";
import { deploy, startLocalJazzServer } from "jazz-tools/testing";
import { app } from "../../schema.js";
import permissions from "../../permissions.js";
import { createChat, shareChat, membershipId } from "../../src/chat.js";

function store(): AccountStore {
  let value: string | null = null;
  return {
    async read() {
      return value;
    },
    async update(transform) {
      value = transform(value);
    },
  };
}

it("keeps room administration with its immutable owner while recipients can send and sharing can be retried", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Db[] = [];
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const configs = await Promise.all(
      Array.from({ length: 3 }, async () => {
        const manager = await createAccountManager({
          appId: server.appId,
          serverUrl: server.url,
          store: store(),
        });
        return {
          appId: server.appId,
          serverUrl: server.url,
          account: manager.createLocalFirst(),
          driver: { type: "memory" as const },
          e2ee: { app, store: store() },
        };
      }),
    );
    const owner = await createDb(configs[0]!);
    clients.push(owner);
    const chat = await createChat(owner, configs[0]!.account.id);
    // Membership is accepted but this recipient has not enrolled an E2EE device yet.
    await expect(shareChat(owner, chat.id, configs[1]!.account.id)).rejects.toThrow();
    expect(
      await owner.all(
        app.chatMembers.where({ chatId: chat.id, accountId: configs[1]!.account.id }),
        { tier: "global" },
      ),
    ).toEqual([
      {
        id: membershipId(chat.id, configs[1]!.account.id),
        chatId: chat.id,
        accountId: configs[1]!.account.id,
      },
    ]);
    const recipient = await createDb(configs[1]!);
    clients.push(recipient);
    const outsider = await createDb(configs[2]!);
    clients.push(outsider);
    await recipient.e2ee.devices.list();
    await outsider.e2ee.devices.list();
    await shareChat(owner, chat.id, configs[1]!.account.id);
    await shareChat(owner, chat.id, configs[1]!.account.id);
    expect(
      await owner.all(
        app.chatMembers.where({ chatId: chat.id, accountId: configs[1]!.account.id }),
        { tier: "global" },
      ),
    ).toEqual([
      {
        id: membershipId(chat.id, configs[1]!.account.id),
        chatId: chat.id,
        accountId: configs[1]!.account.id,
      },
    ]);
    await recipient
      .insert(app.messages, {
        chatId: chat.id,
        senderId: configs[1]!.account.id,
        text: "A recipient can send",
        filename: null,
        mimeType: null,
        payload: null,
      })
      .wait({ tier: "global" });
    expect(
      await owner.all(app.messages.where({ chatId: chat.id }), { tier: "global" }),
    ).toContainEqual(expect.objectContaining({ text: "A recipient can send" }));
    await expect(
      recipient.e2ee.spaces.grant(app.chats, chat.id, configs[2]!.account.id).wait(),
    ).rejects.toThrow();
    await expect(
      recipient.e2ee.spaces.revoke(app.chats, chat.id, configs[0]!.account.id).wait(),
    ).rejects.toThrow();
    await expect(
      recipient
        .update(app.chats, chat.id, { ownerId: configs[1]!.account.id })
        .wait({ tier: "global" }),
    ).rejects.toThrow();
    await expect(
      outsider.insert(app.chats, { ownerId: configs[0]!.account.id }).wait({ tier: "global" }),
    ).rejects.toThrow();
    expect(await outsider.all(app.messages.where({ chatId: chat.id }), { tier: "global" })).toEqual(
      [],
    );
    expect(await outsider.all(app.chats.where({ id: chat.id }), { tier: "global" })).toEqual([]);
    expect(await owner.all(app.__e2ee_space_successors, { tier: "global" })).toEqual([]);
    expect(
      await owner.all(app.__e2ee_space_grants.where({ recipientId: configs[2]!.account.id }), {
        tier: "global",
      }),
    ).toEqual([]);
  } finally {
    await Promise.all(clients.map((db) => db.shutdown()));
    await server.stop();
  }
}, 60_000);
