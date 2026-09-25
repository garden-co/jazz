import { expect, it } from "vitest";
import {
  createAccountManager,
  createDb,
  schema as s,
  type AccountStore,
  type Db,
} from "jazz-tools";
import { deviceRequestSchema, groupSchema, spaceSchema } from "jazz-tools/e2ee";
import { deploy, startLocalJazzServer } from "jazz-tools/testing";
import { app } from "../../schema.js";
import permissions from "../../permissions.js";
import { createChat, shareChat, membershipId } from "../../src/chat.js";

// Same physical schema, intentionally without automatic content decryption.
const physical = s.defineApp({
  ...deviceRequestSchema,
  ...groupSchema,
  ...spaceSchema,
  chats: s.table({ ownerId: s.uuid() }, {}),
  chatOwners: s.table(
    { chatId: s.uuid(), accountId: s.uuid() },
    { chat: s.rel("chats", "chatId"), account: s.rel("__e2ee_account_identities", "accountId") },
  ),
  chatMembers: s.table(
    { chatId: s.uuid(), accountId: s.uuid() },
    { chat: s.rel("chats", "chatId") },
  ),
  messages: s.table(
    {
      chatId: s.uuid(),
      senderId: s.uuid(),
      text: s.bytes(),
      filename: s.bytes().optional(),
      mimeType: s.bytes().optional(),
      payload: s.bytes().optional(),
    },
    { chat: s.rel("chats", "chatId") },
  ),
});

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
    const chat = await createChat(owner, configs[0]!.account.id).catch((cause) => {
      throw new Error("Room creation was rejected", { cause });
    });
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
    // An unauthorized candidate must not authorize descendants or partially
    // publish an otherwise legitimate room in the same exclusive transaction.
    const forged = outsider.beginExclusiveTransaction();
    const forgedChat = forged.insert(app.chats, { ownerId: configs[2]!.account.id });
    const forgedBindingId = crypto.randomUUID();
    forged.upsert(app.chatOwners, forgedBindingId, {
      chatId: forgedChat.id,
      accountId: configs[0]!.account.id,
    });
    const forgedMembershipId = membershipId(forgedChat.id, configs[2]!.account.id);
    forged.upsert(app.chatMembers, forgedMembershipId, {
      chatId: forgedChat.id,
      accountId: configs[2]!.account.id,
    });
    await expect(forged.commit().wait({ tier: "global" })).rejects.toThrow();
    expect(await outsider.all(app.chats.where({ id: forgedChat.id }), { tier: "global" })).toEqual(
      [],
    );
    expect(
      await owner.all(app.chatOwners.where({ id: forgedBindingId }), { tier: "global" }),
    ).toEqual([]);
    expect(
      await outsider.all(app.chatMembers.where({ id: forgedMembershipId }), { tier: "global" }),
    ).toEqual([]);
    expect(
      await owner.all(app.__e2ee_spaces.where({ identifier: forgedChat.id }), { tier: "global" }),
    ).toEqual([]);
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
    const root = await owner.one(app.__e2ee_spaces.where({ identifier: chat.id }), {
      tier: "global",
    });
    expect(root).not.toBeNull();
    await expect(
      outsider
        .insert(app.chatOwners, {
          chatId: chat.id,
          accountId: configs[2]!.account.id,
        })
        .wait({ tier: "global" }),
    ).rejects.toThrow(/AuthorizationDenied|Write rejected by server authorization/);
    const { id: canonicalRootId, ...rootValues } = root!;
    const duplicateRootId = crypto.randomUUID();
    await expect(
      outsider
        .insert(
          app.__e2ee_spaces,
          {
            ...rootValues,
            accountId: configs[2]!.account.id,
          },
          { id: duplicateRootId },
        )
        .wait({ tier: "global" }),
    ).rejects.toThrow(/AuthorizationDenied|Write rejected by server authorization/);
    expect(
      await owner.all(app.__e2ee_spaces.where({ identifier: chat.id }), { tier: "global" }),
    ).toEqual([{ id: canonicalRootId, ...rootValues }]);
    // Exercise the ordinary successor write policy directly: revoke() above can
    // fail at its earlier grant-removal step before it attempts a successor.
    await expect(
      recipient
        .insert(app.__e2ee_space_successors, {
          spaceId: root!.id,
          predecessor: root!.epochId,
          epochId: crypto.randomUUID(),
          authorAccountId: configs[1]!.account.id,
          authorDeviceId: root!.deviceId,
          authorEpochId: root!.accountEpochId,
          revision: new Uint8Array(),
          membership: new Uint8Array(),
          verification: new Uint8Array(),
          history: new Uint8Array(),
          authorEnvelope: new Uint8Array(),
          signature: new Uint8Array(),
        })
        .wait({ tier: "global" }),
    ).rejects.toThrow(/AuthorizationDenied|Write rejected by server authorization/);
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
    const imageBytes = new Uint8Array([137, 80, 78, 71, 13, 10, 26, 10, 91, 42]);
    const image = await owner.insertStreaming(app.messages, {
      chatId: chat.id,
      senderId: configs[0]!.account.id,
      text: "Private image caption",
      filename: "secret-image.png",
      mimeType: "image/png",
      payload: new File([imageBytes], "secret-image.png", { type: "image/png" }).stream(),
    });
    await image.wait({ tier: "global" });
    // Grant only ordinary row visibility to the observer, not an encryption key.
    await owner
      .upsert(app.chatMembers, membershipId(chat.id, configs[2]!.account.id), {
        chatId: chat.id,
        accountId: configs[2]!.account.id,
      })
      .wait({ tier: "global" });
    const stored = await outsider.one(physical.messages.where({ id: image.value.id }), {
      tier: "global",
    });
    expect(stored).not.toBeNull();
    expect(stored!.payload).not.toEqual(imageBytes);
    expect(new TextDecoder().decode(stored!.text)).not.toContain("Private image caption");
    expect(new TextDecoder().decode(stored!.filename!)).not.toContain("secret-image.png");
    expect(new TextDecoder().decode(stored!.mimeType!)).not.toContain("image/png");
    await expect(
      outsider.one(app.messages.where({ id: image.value.id }), { tier: "global" }),
    ).rejects.toThrow();
  } finally {
    await Promise.all(clients.map((db) => db.shutdown()));
    await server.stop();
  }
}, 60_000);
