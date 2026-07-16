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
    const privateChat = testApp.seed((db) => {
      const { value: privateChat } = db.insert(app.chats, {
        name: "Private room",
        isPublic: false,
        createdBy: "alice",
        joinCode: "invite-123",
      });
      db.insert(app.chatMembers, {
        chatId: privateChat.id,
        userId: "alice",
        joinCode: "invite-123",
      });
      return privateChat;
    });

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
    const privateChat = testApp.seed((db) => {
      db.insert(app.profiles, {
        userId: "alice",
        name: "Alice",
      });
      const { value: privateChat } = db.insert(app.chats, {
        name: "Members only",
        isPublic: false,
        createdBy: "alice",
        joinCode: "invite-456",
      });
      db.insert(app.chatMembers, {
        chatId: privateChat.id,
        userId: "alice",
        joinCode: "invite-456",
      });
      return privateChat;
    });

    const aliceDb = testApp.as({ user_id: "alice", claims: {}, authMode: "local-first" });

    aliceDb.expectAllowed((db) =>
      db.update(app.chats, privateChat.id, {
        name: "New chat title",
      }),
    );
  });

  it("does not allow chat creator and isPublic updates", async () => {
    const privateChat = testApp.seed((db) => {
      db.insert(app.profiles, {
        userId: "alice",
        name: "Alice",
      });
      const { value: privateChat } = db.insert(app.chats, {
        name: "Members only",
        isPublic: false,
        createdBy: "alice",
        joinCode: "invite-456",
      });
      db.insert(app.chatMembers, {
        chatId: privateChat.id,
        userId: "alice",
        joinCode: "invite-456",
      });
      return privateChat;
    });

    const aliceDb = testApp.as({ user_id: "alice", claims: {}, authMode: "local-first" });

    aliceDb.expectDenied((db) =>
      db.update(app.chats, privateChat.id, {
        createdBy: "bob",
      }),
    );

    aliceDb.expectDenied((db) =>
      db.update(app.chats, privateChat.id, {
        isPublic: true,
      }),
    );
  });

  it("allows message inserts only for chat members", async () => {
    const { aliceProfile, bobProfile, privateChat } = testApp.seed((db) => {
      const { value: aliceProfile } = db.insert(app.profiles, {
        userId: "alice",
        name: "Alice",
      });
      const { value: bobProfile } = db.insert(app.profiles, {
        userId: "bob",
        name: "Bob",
      });
      const { value: privateChat } = db.insert(app.chats, {
        name: "Members only",
        isPublic: false,
        createdBy: "alice",
        joinCode: "invite-456",
      });
      db.insert(app.chatMembers, {
        chatId: privateChat.id,
        userId: "alice",
        joinCode: "invite-456",
      });
      return { aliceProfile, bobProfile, privateChat };
    });

    const aliceDb = testApp.as({ user_id: "alice", claims: {}, authMode: "local-first" });
    const bobDb = testApp.as({ user_id: "bob", claims: {}, authMode: "local-first" });

    aliceDb.expectAllowed((db) =>
      db.insert(app.messages, {
        chatId: privateChat.id,
        text: "hello from alice",
        senderId: aliceProfile.id,
        createdAt: new Date("2026-01-01T00:00:00.000Z"),
      }),
    );

    bobDb.expectDenied((db) =>
      db.insert(app.messages, {
        chatId: privateChat.id,
        text: "hello from bob",
        senderId: bobProfile.id,
        createdAt: new Date("2026-01-01T00:00:01.000Z"),
      }),
    );

    testApp.seed((db) => {
      db.insert(app.chatMembers, {
        chatId: privateChat.id,
        userId: "bob",
        joinCode: "invite-456",
      });
    });

    bobDb.expectAllowed((db) =>
      db.insert(app.messages, {
        chatId: privateChat.id,
        text: "hello from bob after joining",
        senderId: bobProfile.id,
        createdAt: new Date("2026-01-01T00:00:02.000Z"),
      }),
    );
  });

  it("inherits reaction reads from the parent message/chat chain", async () => {
    const reaction = testApp.seed((db) => {
      const { value: aliceProfile } = db.insert(app.profiles, {
        userId: "alice",
        name: "Alice",
      });
      const { value: privateChat } = db.insert(app.chats, {
        name: "Uploads",
        isPublic: false,
        createdBy: "alice",
        joinCode: "invite-789",
      });
      db.insert(app.chatMembers, {
        chatId: privateChat.id,
        userId: "alice",
        joinCode: "invite-789",
      });
      db.insert(app.chatMembers, {
        chatId: privateChat.id,
        userId: "bob",
        joinCode: "invite-789",
      });
      const { value: message } = db.insert(app.messages, {
        chatId: privateChat.id,
        text: "see attachment",
        senderId: aliceProfile.id,
        createdAt: new Date("2026-01-01T00:00:03.000Z"),
      });
      const { value: reaction } = db.insert(app.reactions, {
        messageId: message.id,
        userId: "alice",
        emoji: "fire",
      });
      return reaction;
    });

    const bobDb = testApp.as({ user_id: "bob", claims: {}, authMode: "local-first" });
    const carolDb = testApp.as({ user_id: "carol", claims: {}, authMode: "local-first" });

    await expect(bobDb.all(app.reactions.where({ id: reaction.id }))).resolves.toEqual([
      expect.objectContaining({ id: reaction.id, emoji: "fire" }),
    ]);
    await expect(carolDb.all(app.reactions.where({ id: reaction.id }))).resolves.toEqual([]);
  });

  it("lets the sender delete their own message but no one else", async () => {
    const message = testApp.seed((db) => {
      const { value: aliceProfile } = db.insert(app.profiles, {
        userId: "alice",
        name: "Alice",
      });
      const { value: chat } = db.insert(app.chats, {
        name: "Members only",
        isPublic: false,
        createdBy: "alice",
        joinCode: "del-1",
      });
      db.insert(app.chatMembers, { chatId: chat.id, userId: "alice", joinCode: "del-1" });
      db.insert(app.chatMembers, { chatId: chat.id, userId: "bob", joinCode: "del-1" });
      const { value: message } = db.insert(app.messages, {
        chatId: chat.id,
        text: "alice's message",
        senderId: aliceProfile.id,
        createdAt: new Date("2026-01-01T00:00:04.000Z"),
      });
      return message;
    });

    const aliceDb = testApp.as({ user_id: "alice", claims: {}, authMode: "local-first" });
    const bobDb = testApp.as({ user_id: "bob", claims: {}, authMode: "local-first" });

    aliceDb.expectAllowed((db) => db.delete(app.messages, message.id));
    bobDb.expectDenied((db) => db.delete(app.messages, message.id));
  });

  it("lets the sender cascade-delete a message's attachments, files and reactions", async () => {
    const seeded = testApp.seed((db) => {
      const { value: aliceProfile } = db.insert(app.profiles, {
        userId: "alice",
        name: "Alice",
      });
      const { value: chat } = db.insert(app.chats, {
        name: "Uploads",
        isPublic: false,
        createdBy: "alice",
        joinCode: "del-2",
      });
      db.insert(app.chatMembers, { chatId: chat.id, userId: "alice", joinCode: "del-2" });
      db.insert(app.chatMembers, { chatId: chat.id, userId: "bob", joinCode: "del-2" });
      const { value: message } = db.insert(app.messages, {
        chatId: chat.id,
        text: "see attachment",
        senderId: aliceProfile.id,
        createdAt: new Date("2026-01-01T00:00:05.000Z"),
      });
      const { value: filePart } = db.insert(app.file_parts, {
        data: new Uint8Array([1, 2, 3]),
      });
      const { value: file } = db.insert(app.files, {
        name: "note.txt",
        mimeType: "text/plain",
        partIds: [filePart.id],
        partSizes: [3],
      });
      const { value: attachment } = db.insert(app.attachments, {
        messageId: message.id,
        type: "file",
        name: "note.txt",
        fileId: file.id,
        size: 3,
      });
      // A reaction left by someone *other* than the sender must also be removable
      // when the sender deletes the message it hangs off.
      const { value: bobReaction } = db.insert(app.reactions, {
        messageId: message.id,
        userId: "bob",
        emoji: "🔥",
      });
      return { message, filePart, file, attachment, bobReaction };
    });

    const aliceDb = testApp.as({ user_id: "alice", claims: {}, authMode: "local-first" });
    const carolDb = testApp.as({ user_id: "carol", claims: {}, authMode: "local-first" });

    aliceDb.expectAllowed((db) => {
      db.delete(app.file_parts, seeded.filePart.id);
      db.delete(app.files, seeded.file.id);
      db.delete(app.attachments, seeded.attachment.id);
      db.delete(app.reactions, seeded.bobReaction.id);
      db.delete(app.messages, seeded.message.id);
    });

    carolDb.expectDenied((db) => db.delete(app.attachments, seeded.attachment.id));
  });
});
