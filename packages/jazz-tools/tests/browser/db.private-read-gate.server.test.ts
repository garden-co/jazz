import { afterEach, describe, expect, it } from "vitest";
import {
  createDb,
  generateAuthSecret,
  publishStoredPermissions,
  schema,
  type CompiledPermissions,
  type Db,
  type RowOf,
} from "../../src/index.js";
import { fetchPermissionsHead, publishStoredSchema } from "../../src/runtime/schema-fetch.js";
import {
  TestCleanup,
  sleep,
  uniqueDbName,
  waitForCondition,
  waitForQuery,
  withTimeout,
} from "./support.js";
import { getJazzServerInfo } from "./testing-server.js";

const app = schema.defineApp({
  chats: schema.table({
    title: schema.string(),
    visibility: schema.string(),
    owner_id: schema.string(),
  }),
  chat_members: schema.table({
    chat_id: schema.ref("chats"),
    user_id: schema.string(),
  }),
  messages: schema.table({
    chat_id: schema.ref("chats"),
    body: schema.string(),
    author_id: schema.string(),
    owner_id: schema.string(),
  }),
  announcements: schema.table({
    title: schema.string(),
  }),
});

const permissions = schema.definePermissions(app, ({ policy, anyOf, session }) => [
  policy.chats.allowRead.where(anyOf([{ visibility: "public" }, { owner_id: session.user_id }])),
  policy.chats.allowInsert.always(),
  policy.chats.allowUpdate.always(),
  policy.chats.allowDelete.always(),

  policy.chat_members.allowRead.where({ user_id: session.user_id }),
  policy.chat_members.allowInsert.always(),
  policy.chat_members.allowUpdate.always(),
  policy.chat_members.allowDelete.always(),

  policy.messages.allowRead.where({ owner_id: session.user_id }),
  policy.messages.allowInsert.always(),
  policy.messages.allowUpdate.always(),
  policy.messages.allowDelete.always(),

  policy.announcements.allowRead.always(),
  policy.announcements.allowInsert.always(),
  policy.announcements.allowUpdate.always(),
  policy.announcements.allowDelete.always(),
]);

const chatStyleMessagePermissions = schema.definePermissions(app, ({ policy, anyOf, session }) => [
  policy.chats.allowRead.where((chat) =>
    anyOf([
      { visibility: "public" },
      policy.chat_members.exists.where({
        chat_id: chat.id,
        user_id: session.user_id,
      }),
    ]),
  ),
  policy.chats.allowInsert.always(),
  policy.chats.allowUpdate.always(),
  policy.chats.allowDelete.always(),

  policy.chat_members.allowRead.where({ user_id: session.user_id }),
  policy.chat_members.allowInsert.where({ user_id: session.user_id }),
  policy.chat_members.allowUpdate.always(),
  policy.chat_members.allowDelete.where({ user_id: session.user_id }),

  policy.messages.allowRead.where((message) =>
    anyOf([
      policy.chats.exists.where({
        id: message.chat_id,
        visibility: "public",
      }),
      policy.chat_members.exists.where({
        chat_id: message.chat_id,
        user_id: session.user_id,
      }),
    ]),
  ),
  policy.messages.allowInsert.where((message) =>
    policy.chat_members.exists.where({
      chat_id: message.chat_id,
      user_id: session.user_id,
    }),
  ),
  policy.messages.allowUpdate.always(),
  policy.messages.allowDelete.always(),

  policy.announcements.allowRead.always(),
  policy.announcements.allowInsert.always(),
  policy.announcements.allowUpdate.always(),
  policy.announcements.allowDelete.always(),
]);

type Chat = RowOf<typeof app.chats>;
type Message = RowOf<typeof app.messages>;

type BobExposureSnapshot = {
  phase: string;
  chatCount: number;
  messageCount: number;
  privateChatVisibleToBob: boolean;
  privateMessageVisibleToBob: boolean;
};

const ctx = new TestCleanup();

afterEach(async () => {
  await ctx.cleanup();
});

describe("raw websocket private read gate", () => {
  it("reads messages through public-chat or membership read policy after edge writes", async () => {
    const { appId, serverUrl, adminSecret } = await getJazzServerInfo(
      uniqueDbName("chat-style-message-read"),
    );
    await publishSchemaAndPermissions(appId, serverUrl, adminSecret, chatStyleMessagePermissions);

    const alice = await openUserDb(appId, serverUrl, "chat-style-message-alice");
    const bob = await openUserDb(appId, serverUrl, "chat-style-message-bob");
    const aliceUserId = requireUserId(alice, "Alice");
    const bobUserId = requireUserId(bob, "Bob");

    const publicChat = await alice
      .insert(app.chats, {
        title: `public-chat-${Date.now()}`,
        visibility: "public",
        owner_id: aliceUserId,
      })
      .wait({ tier: "edge" });
    await alice
      .insert(app.chat_members, {
        chat_id: publicChat.id,
        user_id: aliceUserId,
      })
      .wait({ tier: "edge" });
    const publicMessage = await alice
      .insert(app.messages, {
        chat_id: publicChat.id,
        body: "public chat message",
        author_id: aliceUserId,
        owner_id: aliceUserId,
      })
      .wait({ tier: "edge" });

    await expect(
      waitForQuery(
        bob,
        app.chats,
        (rows) => rows.some((row) => row.id === publicChat.id),
        "Bob should read the public chat dependency",
        15_000,
        "edge",
      ),
    ).resolves.toBeDefined();

    await expect(
      waitForQuery(
        bob,
        app.messages.where({ chat_id: publicChat.id }),
        (rows) => rows.some((row) => row.id === publicMessage.id),
        "Bob should read a public-chat message through the message read policy",
        15_000,
        "edge",
      ),
    ).resolves.toBeDefined();

    await bob
      .insert(app.chat_members, {
        chat_id: publicChat.id,
        user_id: bobUserId,
      })
      .wait({ tier: "edge" });
    const bobMessage = await bob
      .insert(app.messages, {
        chat_id: publicChat.id,
        body: "bob member message",
        author_id: bobUserId,
        owner_id: bobUserId,
      })
      .wait({ tier: "edge" });

    await expect(
      waitForQuery(
        bob,
        app.messages.where({ chat_id: publicChat.id }),
        (rows) => rows.some((row) => row.id === bobMessage.id),
        "Bob should read his member message after an edge-confirmed membership",
        15_000,
        "edge",
      ),
    ).resolves.toBeDefined();
  }, 60_000);

  it("does not expose Alice's private chat or message rows to Bob without adminSecret", async () => {
    const { appId, serverUrl, adminSecret } = await getJazzServerInfo(
      uniqueDbName("private-read-gate"),
    );
    await publishSchemaAndPermissions(appId, serverUrl, adminSecret, permissions);

    const alice = await openUserDb(appId, serverUrl, "private-read-gate-alice");
    const bob = await openUserDb(appId, serverUrl, "private-read-gate-bob");
    const aliceUserId = requireUserId(alice, "Alice");

    const privateChat = await withTimeout(
      alice
        .insert(app.chats, {
          title: `private-chat-${Date.now()}`,
          visibility: "private",
          owner_id: aliceUserId,
        })
        .wait({ tier: "edge" }),
      10_000,
      "Alice private chat insert did not reach the server",
    );
    await withTimeout(
      alice
        .insert(app.chat_members, {
          chat_id: privateChat.id,
          user_id: aliceUserId,
        })
        .wait({ tier: "edge" }),
      10_000,
      "Alice private membership insert did not reach the server",
    );
    const privateMessage = await withTimeout(
      alice
        .insert(app.messages, {
          chat_id: privateChat.id,
          body: "Alice private message",
          author_id: aliceUserId,
          owner_id: aliceUserId,
        })
        .wait({ tier: "edge" }),
      10_000,
      "Alice private message insert did not reach the server",
    );

    const publicAnnouncement = await withTimeout(
      alice
        .insert(app.announcements, {
          title: `public-control-${Date.now()}`,
        })
        .wait({ tier: "edge" }),
      10_000,
      "Alice public control insert did not reach the server",
    );

    const afterBootstrap = await snapshotBobLocalExposure(
      bob,
      "after Bob bootstrap, before public announcement edge query",
      privateChat.id,
      privateMessage.id,
    );
    expect(afterBootstrap).toMatchObject({
      privateChatVisibleToBob: false,
      privateMessageVisibleToBob: false,
    });

    await waitForQuery(
      bob,
      app.announcements,
      (rows) => rows.some((row) => row.id === publicAnnouncement.id),
      "Bob should see the public control row from the server",
      15_000,
      "edge",
    );

    const afterPublicAnnouncement = await snapshotBobLocalExposure(
      bob,
      "after public announcement edge query",
      privateChat.id,
      privateMessage.id,
    );
    expect(afterPublicAnnouncement).toMatchObject({
      privateChatVisibleToBob: false,
      privateMessageVisibleToBob: false,
    });

    const bobChats = await bob.all(app.chats, { tier: "edge" });
    const afterChatsEdgeQuery = await snapshotBobLocalExposure(
      bob,
      "after private chats edge query",
      privateChat.id,
      privateMessage.id,
    );
    const bobMessages = await bob.all(app.messages, { tier: "edge" });
    const afterMessagesEdgeQuery = await snapshotBobLocalExposure(
      bob,
      "after private messages edge query",
      privateChat.id,
      privateMessage.id,
    );
    expect({
      privateChatVisibleToBob: bobChats.some((row) => row.id === privateChat.id),
      privateMessageVisibleToBob: bobMessages.some((row) => row.id === privateMessage.id),
      localExposureTimeline: [
        afterBootstrap,
        afterPublicAnnouncement,
        afterChatsEdgeQuery,
        afterMessagesEdgeQuery,
      ],
    }).toEqual({
      privateChatVisibleToBob: false,
      privateMessageVisibleToBob: false,
      localExposureTimeline: [
        expect.objectContaining({
          privateChatVisibleToBob: false,
          privateMessageVisibleToBob: false,
        }),
        expect.objectContaining({
          privateChatVisibleToBob: false,
          privateMessageVisibleToBob: false,
        }),
        expect.objectContaining({
          privateChatVisibleToBob: false,
          privateMessageVisibleToBob: false,
        }),
        expect.objectContaining({
          privateChatVisibleToBob: false,
          privateMessageVisibleToBob: false,
        }),
      ],
    });

    const chatSnapshots: Chat[][] = [];
    const messageSnapshots: Message[][] = [];
    const unsubscribeChats = ctx.trackSubscription(
      bob.subscribeAll(
        app.chats,
        (delta) => {
          chatSnapshots.push([...delta.all]);
        },
        { tier: "edge" },
      ),
    );
    const unsubscribeMessages = ctx.trackSubscription(
      bob.subscribeAll(
        app.messages,
        (delta) => {
          messageSnapshots.push([...delta.all]);
        },
        { tier: "edge" },
      ),
    );

    await waitForCondition(
      async () => chatSnapshots.length > 0 && messageSnapshots.length > 0,
      10_000,
      "Bob edge subscriptions should produce initial private table snapshots",
    );
    await sleep(500);

    unsubscribeChats();
    unsubscribeMessages();

    expect(chatSnapshots.flat().some((row) => row.id === privateChat.id)).toBe(false);
    expect(messageSnapshots.flat().some((row) => row.id === privateMessage.id)).toBe(false);
  }, 60_000);
});

async function openUserDb(appId: string, serverUrl: string, label: string): Promise<Db> {
  return ctx.track(
    await createDb({
      appId,
      serverUrl,
      secret: generateAuthSecret(),
      driver: { type: "persistent", dbName: uniqueDbName(label) },
    }),
  );
}

async function publishSchemaAndPermissions(
  appId: string,
  serverUrl: string,
  adminSecret: string,
  permissions: CompiledPermissions,
): Promise<void> {
  const { hash: schemaHash } = await publishStoredSchema(serverUrl, {
    appId,
    adminSecret,
    schema: app.wasmSchema,
  });
  const { head } = await fetchPermissionsHead(serverUrl, {
    appId,
    adminSecret,
  });
  await publishStoredPermissions(serverUrl, {
    appId,
    adminSecret,
    schemaHash,
    permissions,
    expectedParentBundleObjectId: head?.bundleObjectId ?? null,
  });
}

function requireUserId(db: Db, label: string): string {
  const userId = db.getAuthState().session?.user_id;
  if (!userId) {
    throw new Error(`${label} Db did not initialize a local-first session`);
  }
  return userId;
}

async function snapshotBobLocalExposure(
  bob: Db,
  phase: string,
  privateChatId: string,
  privateMessageId: string,
): Promise<BobExposureSnapshot> {
  const [localChats, localMessages] = await Promise.all([
    bob.all(app.chats, { tier: "local", propagation: "local-only" }),
    bob.all(app.messages, { tier: "local", propagation: "local-only" }),
  ]);

  return {
    phase,
    chatCount: localChats.length,
    messageCount: localMessages.length,
    privateChatVisibleToBob: localChats.some((row) => row.id === privateChatId),
    privateMessageVisibleToBob: localMessages.some((row) => row.id === privateMessageId),
  };
}
