import { afterEach, describe, expect, it } from "vitest";
import {
  createDb,
  generateAuthSecret,
  publishStoredPermissions,
  schema,
  type CompiledPermissions,
  type Db,
  type QueryBuilder,
  type RowOf,
} from "../../src/index.js";
import { createJazzClient } from "../../src/web/create-jazz-client.js";
import { createInspectorLocalQueryOptions as inspectorLocalQueryOptions } from "../../src/internal/inspector-query.js";
import { getSubscriptionStore } from "../../src/subscription-store-internal.js";
import { fetchPermissionsHead, publishStoredSchema } from "../../src/runtime/schema-fetch.js";
import {
  TestCleanup,
  sleep,
  uniqueDbName,
  waitForCondition,
  waitForQuery,
  withTimeout,
} from "./support.js";
import { getJazzServerInfo, getJazzServerJwtForUser } from "./testing-server.js";

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

const camelChatApp = schema.defineApp({
  chats: schema.table({
    name: schema.string().optional(),
    isPublic: schema.boolean(),
    createdBy: schema.string(),
    joinCode: schema.string().optional(),
  }),
  profiles: schema.table({
    userId: schema.string(),
    name: schema.string(),
    avatar: schema.string().optional(),
  }),
  chatMembers: schema.table({
    chatId: schema.ref("chats"),
    userId: schema.string(),
    joinCode: schema.string().optional(),
  }),
  messages: schema.table({
    chatId: schema.ref("chats"),
    senderId: schema.ref("profiles"),
    text: schema.string(),
    createdAt: schema.timestamp(),
  }),
  reactions: schema.table({
    messageId: schema.ref("messages"),
    userId: schema.string(),
    emoji: schema.string(),
  }),
});

const permissions = schema.definePermissions(app, ({ policy, anyOf, session }) => [
  policy.chats.allowRead.where(anyOf([{ visibility: "public" }, { owner_id: session.user }])),
  policy.chats.allowInsert.always(),
  policy.chats.allowUpdate.always(),
  policy.chats.allowDelete.always(),

  policy.chat_members.allowRead.where({ user_id: session.user }),
  policy.chat_members.allowInsert.always(),
  policy.chat_members.allowUpdate.always(),
  policy.chat_members.allowDelete.always(),

  policy.messages.allowRead.where({ owner_id: session.user }),
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
        user_id: session.user,
      }),
    ]),
  ),
  policy.chats.allowInsert.always(),
  policy.chats.allowUpdate.always(),
  policy.chats.allowDelete.always(),

  policy.chat_members.allowRead.where({ user_id: session.user }),
  policy.chat_members.allowInsert.where({ user_id: session.user }),
  policy.chat_members.allowUpdate.always(),
  policy.chat_members.allowDelete.where({ user_id: session.user }),

  policy.messages.allowRead.where((message) =>
    anyOf([
      policy.chats.exists.where({
        id: message.chat_id,
        visibility: "public",
      }),
      policy.chat_members.exists.where({
        chat_id: message.chat_id,
        user_id: session.user,
      }),
    ]),
  ),
  policy.messages.allowInsert.where((message) =>
    policy.chat_members.exists.where({
      chat_id: message.chat_id,
      user_id: session.user,
    }),
  ),
  policy.messages.allowUpdate.always(),
  policy.messages.allowDelete.always(),

  policy.announcements.allowRead.always(),
  policy.announcements.allowInsert.always(),
  policy.announcements.allowUpdate.always(),
  policy.announcements.allowDelete.always(),
]);

const camelChatStyleMessagePermissions = schema.definePermissions(
  camelChatApp,
  ({ policy, anyOf, allowedTo, session }) => [
    policy.profiles.allowRead.where({}),
    policy.profiles.allowInsert.where({ userId: session.user }),
    policy.profiles.allowUpdate.where({ userId: session.user }),

    policy.chats.allowRead.where((chat) =>
      anyOf([
        { isPublic: true },
        policy.chatMembers.exists.where({
          chatId: chat.id,
          userId: session.user,
        }),
        { joinCode: session.claims["join_code"] },
      ]),
    ),
    policy.chats.allowInsert.where({ createdBy: session.user }),
    policy.chats.allowUpdate.where({ createdBy: session.user }),
    policy.chats.allowDelete.where({ createdBy: session.user }),

    policy.chatMembers.allowRead.where((member) =>
      anyOf([
        { userId: session.user },
        policy.chatMembers.exists.where({
          chatId: member.chatId,
          userId: session.user,
        }),
      ]),
    ),
    policy.chatMembers.allowInsert.where({ userId: session.user }),
    policy.chatMembers.allowUpdate.always(),
    policy.chatMembers.allowDelete.where({ userId: session.user }),

    policy.messages.allowRead.where((message) =>
      anyOf([
        policy.chats.exists.where({ id: message.chatId, isPublic: true }),
        policy.chatMembers.exists.where({
          chatId: message.chatId,
          userId: session.user,
        }),
      ]),
    ),
    policy.messages.allowInsert.where((message) =>
      policy.chatMembers.exists.where({
        chatId: message.chatId,
        userId: session.user,
      }),
    ),
    // `senderId` is a profile UUID; policy evaluation retains the native
    // subject claim for this typed internal binding. Public ownership fields
    // above use the canonical public `session.user` identity.
    policy.messages.allowDelete.where({ senderId: session.claims["sub"] }),

    policy.reactions.allowRead.where(allowedTo.read("messageId")),
    policy.reactions.allowInsert.where({ userId: session.user }),
    policy.reactions.allowDelete.where({ userId: session.user }),
  ],
);

const createdByApp = schema.defineApp({
  todos: schema.table({
    title: schema.string(),
    done: schema.boolean(),
  }),
});

const createdByPermissions = schema.definePermissions(createdByApp, ({ policy, session }) => {
  policy.todos.allowRead.where({ $createdBy: session.user });
  policy.todos.allowInsert.always();
  policy.todos.allowUpdate.where({ $createdBy: session.user });
  policy.todos.allowDelete.where({ $createdBy: session.user });
});

type Chat = RowOf<typeof app.chats>;
type Message = RowOf<typeof app.messages>;
type CreatedByTodo = RowOf<typeof createdByApp.todos>;

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
  it("delivers main-thread subscription updates for $createdBy-scoped inserts", async () => {
    const { appId, serverUrl, adminSecret } = await getJazzServerInfo(
      uniqueDbName("created-by-async-channel-subscription"),
    );
    await publishSchemaAndPermissions(
      appId,
      serverUrl,
      adminSecret,
      createdByPermissions,
      createdByApp,
    );

    const client = await createJazzClient({
      appId,
      serverUrl,
      secret: generateAuthSecret(),
      driver: {
        type: "persistent",
        dbName: uniqueDbName("created-by-async-channel-client"),
      },
    });
    ctx.track({
      shutdown: client.shutdown,
    } as Db);

    const store = getSubscriptionStore(client);
    const key = store.makeQueryKey(createdByApp.todos);
    const entry = store.getCacheEntry<CreatedByTodo>(key);
    const snapshots: CreatedByTodo[][] = [];
    const unsubscribe = entry.subscribe({
      onfulfilled: (rows) => snapshots.push([...rows]),
      onDelta: (delta) => snapshots.push([...delta.all]),
      onReset: () => snapshots.push([]),
    });
    ctx.trackSubscription(unsubscribe);

    await waitForCondition(
      async () => snapshots.length > 0,
      10_000,
      "$createdBy async-channel subscription should produce an initial snapshot",
    );
    await sleep(500);

    const write = client.db.insert(createdByApp.todos, {
      title: "Alice-created async channel row",
      done: false,
    });
    const inserted = await withTimeout(
      write.wait({ tier: "global" }),
      15_000,
      "$createdBy insert global wait",
    );

    await waitForCondition(
      async () => snapshots.some((rows) => rows.some((row) => row.id === inserted.id)),
      15_000,
      "$createdBy async-channel subscription should emit the inserted row",
    );
  }, 60_000);

  it("lets Bob insert a camelCase chat message after edge-confirmed membership", async () => {
    const { appId, serverUrl, adminSecret } = await getJazzServerInfo(
      uniqueDbName("camel-chat-member-message-insert"),
    );
    await publishSchemaAndPermissions(
      appId,
      serverUrl,
      adminSecret,
      camelChatStyleMessagePermissions,
      camelChatApp,
    );

    const alice = await openUserDb(appId, serverUrl, "camel-chat-insert-alice");
    const bob = await openUserDb(appId, serverUrl, "camel-chat-insert-bob");
    const aliceUserId = requireUserId(alice, "Alice");
    const bobUserId = requireUserId(bob, "Bob");
    await withTimeout(
      alice
        .insert(camelChatApp.profiles, {
          userId: aliceUserId,
          name: "Alice",
        })
        .wait({ tier: "edge" }),
      15_000,
      "Alice profile edge wait",
    );
    const bobProfile = await withTimeout(
      bob
        .insert(camelChatApp.profiles, {
          userId: bobUserId,
          name: "Bob",
        })
        .wait({ tier: "edge" }),
      15_000,
      "Bob profile edge wait",
    );
    const chat = await withTimeout(
      alice
        .insert(camelChatApp.chats, {
          name: `camel-chat-${Date.now()}`,
          isPublic: true,
          createdBy: aliceUserId,
        })
        .wait({ tier: "edge" }),
      15_000,
      "Alice chat edge wait",
    );
    await withTimeout(
      alice
        .insert(camelChatApp.chatMembers, {
          chatId: chat.id,
          userId: aliceUserId,
        })
        .wait({ tier: "edge" }),
      15_000,
      "Alice membership edge wait",
    );
    await withTimeout(
      alice
        .insert(camelChatApp.messages, {
          chatId: chat.id,
          senderId: bobProfile.id,
          text: "hello from seed chat",
          createdAt: new Date(),
        })
        .wait({ tier: "edge" }),
      15_000,
      "Alice seed message edge wait",
    );
    await withTimeout(
      bob
        .insert(camelChatApp.chatMembers, {
          chatId: chat.id,
          userId: bobUserId,
        })
        .wait({ tier: "edge" }),
      15_000,
      "Bob membership edge wait",
    );

    await expect(
      waitForQuery(
        bob,
        camelChatApp.chatMembers.where({ chatId: chat.id }),
        (rows) => rows.some((row) => row.userId === bobUserId),
        "Bob should read the camelCase chat member list after joining",
        15_000,
        "edge",
      ),
    ).resolves.toBeDefined();

    const bobMessage = await withTimeout(
      bob
        .insert(camelChatApp.messages, {
          chatId: chat.id,
          senderId: bobProfile.id,
          text: "bob member message",
          createdAt: new Date(),
        })
        .wait({ tier: "edge" }),
      15_000,
      "Bob message edge wait",
    );

    await expect(
      waitForQuery(
        bob,
        camelChatApp.messages
          .where({ chatId: chat.id })
          .include({ sender: true })
          .orderBy("createdAt", "desc")
          .limit(21),
        (rows) => rows.some((row) => row.id === bobMessage.id),
        "Bob should read his camelCase member message after edge-confirmed membership",
        15_000,
        "edge",
      ),
    ).resolves.toBeDefined();

    await expect(
      waitForQuery(
        bob,
        camelChatApp.reactions.where({ messageId: bobMessage.id }),
        (rows) => rows.length === 0,
        "Bob should settle reaction reads that inherit through camelCase message membership",
        15_000,
        "edge",
      ),
    ).resolves.toBeDefined();

    await expect(
      waitForQuery(
        bob,
        camelChatApp.profiles.where({ id: bobProfile.id }),
        (rows) => rows.some((row) => row.id === bobProfile.id),
        "Bob should read the sender profile mounted by rendered chat messages",
        15_000,
        "edge",
      ),
    ).resolves.toBeDefined();

    const subscriptionQueries = [
      {
        label: "chat list memberships with included chat",
        query: camelChatApp.chatMembers.where({ userId: bobUserId }).include({ chat: true }),
        predicate: (rows: Array<{ id: string }>) => rows.some((row) => row.id !== undefined),
      },
      {
        label: "chat header members",
        query: camelChatApp.chatMembers.where({ chatId: chat.id }),
        predicate: (rows: Array<{ userId?: string }>) =>
          rows.some((row) => row.userId === bobUserId),
      },
      {
        label: "chat display first message",
        query: camelChatApp.messages
          .where({ chatId: chat.id })
          .orderBy("createdAt", "asc")
          .limit(1),
        predicate: (rows: Array<{ id: string }>) => rows.length === 1,
      },
      {
        label: "rendered message reactions",
        query: camelChatApp.reactions.where({ messageId: bobMessage.id }),
        predicate: (rows: unknown[]) => rows.length === 0,
      },
    ] as const;

    const unsubscribeSubscriptions: Array<() => void> = [];
    try {
      await Promise.all(
        subscriptionQueries.map(({ label, query, predicate }) =>
          waitForSubscription(
            bob,
            query,
            predicate as (rows: unknown[]) => boolean,
            `Bob subscription should settle: ${label}`,
            15_000,
            { tier: "edge" },
          ).then((unsubscribe) => {
            unsubscribeSubscriptions.push(unsubscribe);
          }),
        ),
      );
    } finally {
      for (const unsubscribe of unsubscribeSubscriptions.splice(0)) unsubscribe();
    }

    const bobPendingMessage = bob.insert(camelChatApp.messages, {
      chatId: chat.id,
      senderId: bobProfile.id,
      text: "bob local-first message before alice reconnects",
      createdAt: new Date(),
    });
    const bobPendingMessageEdgeWait = withTimeout(
      bobPendingMessage.wait({ tier: "edge" }),
      15_000,
      "Bob fire-and-forget message edge wait",
    );

    const aliceAgain = await openUserDb(appId, serverUrl, "camel-chat-insert-alice-again");
    const aliceAgainUnsubscribe = await Promise.all([
      waitForSubscription(
        aliceAgain,
        camelChatApp.messages
          .where({ chatId: chat.id })
          .include({ sender: true })
          .orderBy("createdAt", "desc")
          .limit(21),
        (rows) => rows.some((row) => row.id === bobPendingMessage.value.id),
        "Alice should receive Bob's fire-and-forget member message through websocket sync",
        15_000,
        { tier: "edge" },
      ),
      bobPendingMessageEdgeWait,
    ]).then(([unsubscribe]) => unsubscribe);
    aliceAgainUnsubscribe();
  }, 60_000);

  it("converts a private chat invite code into normal membership visibility", async () => {
    const { appId, serverUrl, adminSecret } = await getJazzServerInfo(
      uniqueDbName("camel-chat-private-invite"),
    );
    await publishSchemaAndPermissions(
      appId,
      serverUrl,
      adminSecret,
      camelChatStyleMessagePermissions,
      camelChatApp,
    );

    const alice = await openUserDb(appId, serverUrl, "camel-chat-invite-alice");
    // Use the same external issuer/subject for both the invite-scoped and
    // normal sessions. `session.user` deliberately includes that issuer, so a
    // local-first identity is not interchangeable with this JWT identity.
    const bobSubject = "invite-bob";
    const bob = await openJwtUserDb(
      appId,
      serverUrl,
      "camel-chat-invite-bob",
      await getJazzServerJwtForUser(bobSubject, undefined, appId),
    );
    const aliceUserId = requireUserId(alice, "Alice");
    const bobUserId = requireUserId(bob, "Bob");
    const joinCode = `join-${Date.now()}`;

    const aliceProfile = await withTimeout(
      alice
        .insert(camelChatApp.profiles, {
          userId: aliceUserId,
          name: "Alice",
        })
        .wait({ tier: "edge" }),
      15_000,
      "Alice profile edge wait",
    );
    const bobProfile = await withTimeout(
      bob
        .insert(camelChatApp.profiles, {
          userId: bobUserId,
          name: "Bob",
        })
        .wait({ tier: "edge" }),
      15_000,
      "Bob profile edge wait",
    );

    const chat = await withTimeout(
      alice
        .insert(camelChatApp.chats, {
          name: `private-invite-${Date.now()}`,
          isPublic: false,
          createdBy: aliceUserId,
          joinCode,
        })
        .wait({ tier: "edge" }),
      15_000,
      "Alice private invite chat edge wait",
    );
    await withTimeout(
      alice
        .insert(camelChatApp.chatMembers, {
          chatId: chat.id,
          userId: aliceUserId,
        })
        .wait({ tier: "edge" }),
      15_000,
      "Alice private chat membership edge wait",
    );
    const seedMessage = await withTimeout(
      alice
        .insert(camelChatApp.messages, {
          chatId: chat.id,
          senderId: aliceProfile.id,
          text: "invite-only seed",
          createdAt: new Date(),
        })
        .wait({ tier: "edge" }),
      15_000,
      "Alice private seed message edge wait",
    );

    const inviteSession = {
      issuer: "https://issuer.jazz.test",
      user_id: bobSubject,
      claims: { join_code: joinCode },
      authMode: "external" as const,
    };
    const inviteBob = await openJwtUserDb(
      appId,
      serverUrl,
      "camel-chat-invite-bob-scoped",
      await getJazzServerJwtForUser(bobSubject, { join_code: joinCode }, appId),
    );

    await expect(
      waitForQuery(
        inviteBob,
        camelChatApp.chats.where({ id: chat.id }),
        (rows) => rows.some((row) => row.id === chat.id),
        "Bob should query the private chat through the invite-authenticated connection",
        15_000,
        "edge",
      ),
    ).resolves.toBeDefined();

    const unsubscribeInviteRead = await waitForSubscription(
      inviteBob,
      camelChatApp.chats.where({ id: chat.id }),
      (rows) => rows.some((row) => row.id === chat.id),
      "Bob should subscribe to the private chat through the invite claim",
      15_000,
      { tier: "edge" },
      inviteSession,
    );
    unsubscribeInviteRead();

    await withTimeout(
      inviteBob
        .insert(camelChatApp.chatMembers, {
          chatId: chat.id,
          userId: bobUserId,
          joinCode,
        })
        .wait({ tier: "edge" }),
      15_000,
      "Bob invite membership edge wait",
    );

    await expect(
      waitForQuery(
        bob,
        camelChatApp.chats.where({ id: chat.id }),
        (rows) => rows.some((row) => row.id === chat.id),
        "Bob should read the private chat through normal membership after accepting invite",
        15_000,
        "edge",
      ),
    ).resolves.toBeDefined();

    await expect(
      waitForQuery(
        bob,
        camelChatApp.chatMembers.where({ chatId: chat.id }),
        (rows) => rows.some((row) => row.userId === bobUserId),
        "Bob should read his confirmed private chat membership",
        15_000,
        "edge",
      ),
    ).resolves.toBeDefined();

    await expect(
      waitForQuery(
        bob,
        camelChatApp.messages.where({ chatId: chat.id }),
        (rows) => rows.some((row) => row.id === seedMessage.id),
        "Bob should read private seed messages without include/order after accepting invite",
        15_000,
        "edge",
      ),
    ).resolves.toBeDefined();

    await expect(
      waitForQuery(
        bob,
        camelChatApp.messages
          .where({ chatId: chat.id })
          .include({ sender: true })
          .orderBy("createdAt", "asc"),
        (rows) => rows.some((row) => row.id === seedMessage.id),
        "Bob should read private seed messages through normal membership after accepting invite",
        15_000,
        "edge",
      ),
    ).resolves.toBeDefined();

    const unsubscribeBobMessages = await waitForSubscription(
      bob,
      camelChatApp.messages
        .where({ chatId: chat.id })
        .include({ sender: true })
        .orderBy("createdAt", "desc")
        .limit(21),
      (rows) => rows.some((row) => row.id === seedMessage.id),
      "Bob should subscribe to private seed messages through normal membership",
      15_000,
      { tier: "edge" },
    );
    unsubscribeBobMessages();

    const bobMessage = await withTimeout(
      bob
        .insert(camelChatApp.messages, {
          chatId: chat.id,
          senderId: bobProfile.id,
          text: "bob accepted invite",
          createdAt: new Date(),
        })
        .wait({ tier: "edge" }),
      15_000,
      "Bob private invite message edge wait",
    );

    await expect(
      waitForQuery(
        bob,
        camelChatApp.messages.where({ chatId: chat.id }),
        (rows) => rows.some((row) => row.id === bobMessage.id),
        "Bob should read his own private invite message after edge wait",
        15_000,
        "edge",
      ),
    ).resolves.toBeDefined();

    await expect(
      waitForQuery(
        alice,
        camelChatApp.messages
          .where({ chatId: chat.id })
          .include({ sender: true })
          .orderBy("createdAt", "desc"),
        (rows) => rows.some((row) => row.id === bobMessage.id),
        "Alice should receive Bob's private invite message",
        15_000,
        "edge",
      ),
    ).resolves.toBeDefined();
  }, 60_000);

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
      bob.subscribe(
        app.chats,
        (rows) => {
          chatSnapshots.push(rows);
        },
        { tier: "edge" },
      ),
    );
    const unsubscribeMessages = ctx.trackSubscription(
      bob.subscribe(
        app.messages,
        (rows) => {
          messageSnapshots.push(rows);
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

async function openUserDb(
  appId: string,
  serverUrl: string,
  label: string,
  secret = generateAuthSecret(),
): Promise<Db> {
  return ctx.track(
    await createDb({
      appId,
      serverUrl,
      secret,
      driver: { type: "persistent", dbName: uniqueDbName(label) },
    }),
  );
}

async function openJwtUserDb(
  appId: string,
  serverUrl: string,
  label: string,
  jwtToken: string,
): Promise<Db> {
  return ctx.track(
    await createDb({
      appId,
      serverUrl,
      jwtToken,
      driver: { type: "persistent", dbName: uniqueDbName(label) },
    }),
  );
}

async function publishSchemaAndPermissions(
  appId: string,
  serverUrl: string,
  adminSecret: string,
  permissions: CompiledPermissions,
  appSchema: { wasmSchema: typeof app.wasmSchema } = app,
): Promise<void> {
  const { hash: schemaHash } = await publishStoredSchema(serverUrl, {
    appId,
    adminSecret,
    schema: appSchema.wasmSchema,
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
  const userId = db.getAuthState().session?.user;
  if (!userId) {
    throw new Error(`${label} Db did not initialize a local-first session`);
  }
  return userId;
}

async function waitForSubscription<T extends { id: string }>(
  db: Db,
  query: QueryBuilder<T>,
  predicate: (rows: T[]) => boolean,
  label: string,
  timeoutMs = 15_000,
  options?: { tier?: "local" | "edge" },
  session?: {
    issuer: string;
    user_id: string;
    claims: Record<string, unknown>;
    authMode: "external";
  },
): Promise<() => void> {
  return await new Promise<() => void>((resolve, reject) => {
    let settled = false;
    let lastRows: T[] = [];
    let unsubscribe: () => void = () => {};
    const timeoutId = setTimeout(() => {
      if (settled) return;
      settled = true;
      unsubscribe();
      reject(
        new Error(
          `${label}: timed out after ${timeoutMs}ms; lastRows=${JSON.stringify(lastRows.slice(0, 10))}`,
        ),
      );
    }, timeoutMs);
    unsubscribe = db.subscribe(
      query,
      (rows) => {
        if (settled) return;
        lastRows = rows;
        if (!predicate(rows)) return;
        settled = true;
        clearTimeout(timeoutId);
        resolve(unsubscribe);
      },
      options,
      session,
    );
  });
}

async function snapshotBobLocalExposure(
  bob: Db,
  phase: string,
  privateChatId: string,
  privateMessageId: string,
): Promise<BobExposureSnapshot> {
  const [localChats, localMessages] = await Promise.all([
    bob.all(app.chats, inspectorLocalQueryOptions()),
    bob.all(app.messages, inspectorLocalQueryOptions()),
  ]);

  return {
    phase,
    chatCount: localChats.length,
    messageCount: localMessages.length,
    privateChatVisibleToBob: localChats.some((row) => row.id === privateChatId),
    privateMessageVisibleToBob: localMessages.some((row) => row.id === privateMessageId),
  };
}
