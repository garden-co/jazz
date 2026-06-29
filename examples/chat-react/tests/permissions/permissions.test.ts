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

  it("inherits attachment file reads from the parent message/chat chain", async () => {
    const file = testApp.seed((db) => {
      const { value: aliceProfile } = db.insert(app.profiles, {
        userId: "alice",
        name: "Alice",
      });
      const { value: privateChat } = db.insert(app.chats, {
        name: "Uploads",
        isPublic: false,
        createdBy: "alice",
        joinCode: "invite-files",
      });
      db.insert(app.chatMembers, {
        chatId: privateChat.id,
        userId: "alice",
        joinCode: "invite-files",
      });
      db.insert(app.chatMembers, {
        chatId: privateChat.id,
        userId: "bob",
        joinCode: "invite-files",
      });
      const { value: message } = db.insert(app.messages, {
        chatId: privateChat.id,
        text: "see attachment",
        senderId: aliceProfile.id,
        createdAt: new Date("2026-01-01T00:00:04.000Z"),
      });
      const { value: file } = db.insert(app.files, {
        name: "hello.txt",
        mime_type: "text/plain",
        data: new Uint8Array([104, 105]),
      });
      db.insert(app.attachments, {
        messageId: message.id,
        type: "file",
        name: "hello.txt",
        fileId: file.id,
        size: 2,
      });
      return file;
    });

    const bobDb = testApp.as({ user_id: "bob", claims: {}, authMode: "local-first" });
    const carolDb = testApp.as({ user_id: "carol", claims: {}, authMode: "local-first" });

    await expect(bobDb.all(app.files.where({ id: file.id }))).resolves.toEqual([
      expect.objectContaining({
        id: file.id,
        name: "hello.txt",
        mime_type: "text/plain",
        data: new Uint8Array([104, 105]),
      }),
    ]);
    await expect(carolDb.all(app.attachments.where({ fileId: file.id }))).resolves.toEqual([]);
    await expect(carolDb.all(app.files.where({ id: file.id }))).resolves.toEqual([]);
    await expect(carolDb.loadFileAsBlob(app, file.id)).rejects.toMatchObject({
      name: "FileNotFoundError",
    });

    const blob = await bobDb.loadFileAsBlob(app, file.id);
    expect(blob.type).toBe("text/plain");
    expect(Array.from(new Uint8Array(await blob.arrayBuffer()))).toEqual([104, 105]);

    bobDb.expectDenied((db) =>
      db.update(app.files, file.id, {
        mime_type: "text/plain+edited",
      }),
    );
  });

  it("inherits attachment file deletes from the source attachment delete policy", async () => {
    const aliceUserId = "00000000-0000-4000-8000-0000000000a1";
    const bobUserId = "00000000-0000-4000-8000-0000000000b0";
    const { attachment, file } = testApp.seed((db) => {
      const { value: aliceProfile } = db.insert(
        app.profiles,
        {
          userId: aliceUserId,
          name: "Alice",
        },
        { id: aliceUserId },
      );
      db.insert(
        app.profiles,
        {
          userId: bobUserId,
          name: "Bob",
        },
        { id: bobUserId },
      );
      const { value: privateChat } = db.insert(app.chats, {
        name: "Uploads",
        isPublic: false,
        createdBy: aliceUserId,
        joinCode: "invite-delete-files",
      });
      db.insert(app.chatMembers, {
        chatId: privateChat.id,
        userId: aliceUserId,
        joinCode: "invite-delete-files",
      });
      db.insert(app.chatMembers, {
        chatId: privateChat.id,
        userId: bobUserId,
        joinCode: "invite-delete-files",
      });
      const { value: message } = db.insert(app.messages, {
        chatId: privateChat.id,
        text: "delete my attachment",
        senderId: aliceProfile.id,
        createdAt: new Date("2026-01-01T00:00:05.000Z"),
      });
      const { value: file } = db.insert(app.files, {
        name: "owned.txt",
        mime_type: "text/plain",
        data: new Uint8Array([111, 107]),
      });
      const { value: attachment } = db.insert(app.attachments, {
        messageId: message.id,
        type: "file",
        name: "owned.txt",
        fileId: file.id,
        size: 2,
      });
      return { attachment, file };
    });

    const aliceDb = testApp.as({ user_id: aliceUserId, claims: {}, authMode: "local-first" });
    const bobDb = testApp.as({ user_id: bobUserId, claims: {}, authMode: "local-first" });

    bobDb.expectDenied((db) => db.delete(app.attachments, attachment.id));
    bobDb.expectDenied((db) => db.delete(app.files, file.id));
    aliceDb.expectAllowed((db) => db.delete(app.files, file.id));
    aliceDb.expectAllowed((db) => db.delete(app.attachments, attachment.id));
  });
});
