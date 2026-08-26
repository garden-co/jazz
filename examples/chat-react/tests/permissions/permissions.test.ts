import { createPolicyTestApp, type PolicyTestApp } from "jazz-tools/testing";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { app } from "../../schema.js";
import permissions from "../../permissions.js";

let testApp: PolicyTestApp;

beforeEach(async () => {
  testApp = await createPolicyTestApp(app, permissions, expect);
});

afterEach(async () => {
  await testApp?.shutdown();
});

describe("chat permissions", () => {
  it("allows pre-authorized private chat reads via join_code claim", async () => {
    const privateChat = await testApp.seed((db) =>
      db.insert(app.chats, {
        name: "Private room",
        isPublic: false,
        joinCode: "invite-123",
      }),
    );
    await testApp.seed((db) =>
      db.insert(app.chatMembers, {
        chatId: privateChat.id,
        userId: "alice",
        joinCode: "invite-123",
      }),
    );

    const bobWithoutClaim = testApp.as({ user_id: "bob", claims: {}, authMode: "local-first" });
    const bobWithClaim = testApp.as({
      user_id: "bob",
      claims: { join_code: "invite-123" },
      authMode: "local-first",
    });

    await expect(bobWithoutClaim.all(app.chats.where({ id: privateChat.id }))).resolves.toEqual([]);
    await expect(bobWithClaim.all(app.chats.where({ id: privateChat.id }))).resolves.toEqual([
      expect.objectContaining({ id: privateChat.id, name: "Private room" }),
    ]);
  });

  it("allows chat name updates", async () => {
    await testApp.seed((db) =>
      db.insert(app.profiles, {
        userId: "alice",
        name: "Alice",
      }),
    );
    const privateChat = await testApp.seed((db) =>
      db.insert(app.chats, {
        name: "Members only",
        isPublic: false,
        joinCode: "invite-456",
      }),
    );
    await testApp.seed((db) =>
      db.insert(app.chatMembers, {
        chatId: privateChat.id,
        userId: "alice",
        joinCode: "invite-456",
      }),
    );

    const aliceDb = testApp.as({ user_id: "alice", claims: {}, authMode: "local-first" });

    aliceDb.expectAllowed((db) =>
      db.update(app.chats, privateChat.id, {
        name: "New chat title",
      }),
    );
  });

  it("does not allow isPublic updates", async () => {
    await testApp.seed((db) =>
      db.insert(app.profiles, {
        userId: "alice",
        name: "Alice",
      }),
    );
    const privateChat = await testApp.seed((db) =>
      db.insert(app.chats, {
        name: "Members only",
        isPublic: false,
        joinCode: "invite-456",
      }),
    );
    await testApp.seed((db) =>
      db.insert(app.chatMembers, {
        chatId: privateChat.id,
        userId: "alice",
        joinCode: "invite-456",
      }),
    );

    const aliceDb = testApp.as({ user_id: "alice", claims: {}, authMode: "local-first" });

    await aliceDb.expectDenied((db) =>
      db.update(app.chats, privateChat.id, {
        isPublic: true,
      }),
    );
  });

  it("allows message inserts only for chat members", async () => {
    const aliceProfile = await testApp.seed((db) =>
      db.insert(app.profiles, {
        userId: "alice",
        name: "Alice",
      }),
    );
    const bobProfile = await testApp.seed((db) =>
      db.insert(app.profiles, {
        userId: "bob",
        name: "Bob",
      }),
    );
    const privateChat = await testApp.seed((db) =>
      db.insert(app.chats, {
        name: "Members only",
        isPublic: false,
        joinCode: "invite-456",
      }),
    );
    await testApp.seed((db) =>
      db.insert(app.chatMembers, {
        chatId: privateChat.id,
        userId: "alice",
        joinCode: "invite-456",
      }),
    );

    const aliceDb = testApp.as({ user_id: "alice", claims: {}, authMode: "local-first" });
    const bobDb = testApp.as({ user_id: "bob", claims: {}, authMode: "local-first" });

    aliceDb.expectAllowed((db) =>
      db.insert(app.messages, {
        chatId: privateChat.id,
        text: "hello from alice",
        senderId: aliceProfile.id,
      }),
    );

    await bobDb.expectDenied((db) =>
      db.insert(app.messages, {
        chatId: privateChat.id,
        text: "hello from bob",
        senderId: bobProfile.id,
      }),
    );

    await testApp.seed((db) =>
      db.insert(app.chatMembers, {
        chatId: privateChat.id,
        userId: "bob",
        joinCode: "invite-456",
      }),
    );

    bobDb.expectAllowed((db) =>
      db.insert(app.messages, {
        chatId: privateChat.id,
        text: "hello from bob after joining",
        senderId: bobProfile.id,
      }),
    );
  });

  it("binds message attribution to the authenticated user's profile", async () => {
    const aliceProfile = await testApp.seed((db) =>
      db.insert(app.profiles, {
        userId: "alice",
        name: "Alice",
      }),
    );
    const bobProfile = await testApp.seed((db) =>
      db.insert(app.profiles, {
        userId: "bob",
        name: "Bob",
      }),
    );
    const chat = await testApp.seed((db) =>
      db.insert(app.chats, {
        name: "Members only",
        isPublic: false,
        joinCode: "invite-authors",
      }),
    );
    await testApp.seed((db) =>
      db.insert(app.chatMembers, {
        chatId: chat.id,
        userId: "alice",
        joinCode: "invite-authors",
      }),
    );

    const aliceDb = testApp.as({ user_id: "alice", claims: {}, authMode: "local-first" });

    aliceDb.expectAllowed((db) =>
      db.insert(app.messages, {
        chatId: chat.id,
        text: "hello from alice",
        senderId: aliceProfile.id,
      }),
    );
    await aliceDb.expectDenied((db) =>
      db.insert(app.messages, {
        chatId: chat.id,
        text: "forged message from bob",
        senderId: bobProfile.id,
      }),
    );
  });

  it("binds reaction attribution to the authenticated user", async () => {
    const aliceProfile = await testApp.seed((db) =>
      db.insert(app.profiles, {
        userId: "alice",
        name: "Alice",
      }),
    );
    const chat = await testApp.seed((db) =>
      db.insert(app.chats, {
        name: "Members only",
        isPublic: false,
        joinCode: "invite-reactions",
      }),
    );
    await testApp.seed((db) =>
      db.insert(app.chatMembers, {
        chatId: chat.id,
        userId: "alice",
        joinCode: "invite-reactions",
      }),
    );
    const message = await testApp.seed((db) =>
      db.insert(app.messages, {
        chatId: chat.id,
        text: "react to this",
        senderId: aliceProfile.id,
      }),
    );

    const aliceDb = testApp.as({ user_id: "alice", claims: {}, authMode: "local-first" });

    aliceDb.expectAllowed((db) =>
      db.insert(app.reactions, {
        messageId: message.id,
        userId: "alice",
        emoji: "thumbs-up",
      }),
    );
    await aliceDb.expectDenied((db) =>
      db.insert(app.reactions, {
        messageId: message.id,
        userId: "bob",
        emoji: "fire",
      }),
    );
  });

  it("inherits reaction reads from the parent message/chat chain", async () => {
    const aliceProfile = await testApp.seed((db) =>
      db.insert(app.profiles, {
        userId: "alice",
        name: "Alice",
      }),
    );
    const privateChat = await testApp.seed((db) =>
      db.insert(app.chats, {
        name: "Uploads",
        isPublic: false,
        joinCode: "invite-789",
      }),
    );
    await testApp.seed((db) =>
      db.insert(app.chatMembers, {
        chatId: privateChat.id,
        userId: "alice",
        joinCode: "invite-789",
      }),
    );
    await testApp.seed((db) =>
      db.insert(app.chatMembers, {
        chatId: privateChat.id,
        userId: "bob",
        joinCode: "invite-789",
      }),
    );
    const message = await testApp.seed((db) =>
      db.insert(app.messages, {
        chatId: privateChat.id,
        text: "see attachment",
        senderId: aliceProfile.id,
      }),
    );
    const reaction = await testApp.seed((db) =>
      db.insert(app.reactions, {
        messageId: message.id,
        userId: "alice",
        emoji: "fire",
      }),
    );

    const bobDb = testApp.as({ user_id: "bob", claims: {}, authMode: "local-first" });
    const carolDb = testApp.as({ user_id: "carol", claims: {}, authMode: "local-first" });

    await expect(bobDb.all(app.reactions.where({ id: reaction.id }))).resolves.toEqual([
      expect.objectContaining({ id: reaction.id, emoji: "fire" }),
    ]);
    await expect(carolDb.all(app.reactions.where({ id: reaction.id }))).resolves.toEqual([]);
  });
});
