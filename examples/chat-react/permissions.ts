import { definePermissions, RowContext } from "jazz-tools/permissions";
import { app, Chat } from "./schema.js";

export default definePermissions(app, ({ policy, session, allOf, anyOf, allowedTo }) => {
  policy.profiles.allowRead.where({});
  policy.profiles.allowInsert.where({ userId: session.user_id });
  policy.profiles.allowUpdate.where({ userId: session.user_id });

  const userIsChatMember = (chat: RowContext<Chat>) =>
    policy.chatMembers.exists.where({ chatId: chat.id, userId: session.user_id });
  policy.chats.allowRead.where((chat) =>
    anyOf([{ isPublic: true }, userIsChatMember(chat), { joinCode: session["claims.join_code"] }]),
  );
  policy.chats.allowInsert.where({ createdBy: session.user_id });
  policy.chats.allowUpdate.whereOld(userIsChatMember).whereNew((chat) =>
    allOf([
      userIsChatMember(chat),
      // Users may update only non-protected fields. `createdBy` and `isPublic` cannot be updated.
      policy.chats.exists.where({
        id: chat.id,
        createdBy: chat.createdBy,
        isPublic: chat.isPublic,
      }),
    ]),
  );

  policy.chatMembers.allowRead.where((member) =>
    anyOf([
      { userId: session.user_id },
      policy.chatMembers.exists.where({ chatId: member.chatId, userId: session.user_id }),
    ]),
  );
  policy.chatMembers.allowInsert.where({ userId: session.user_id });
  policy.chatMembers.allowDelete.where({ userId: session.user_id });

  policy.messages.allowRead.where((message) =>
    anyOf([
      policy.chats.exists.where({ id: message.chatId, isPublic: true }),
      policy.chatMembers.exists.where({ chatId: message.chatId, userId: session.user_id }),
    ]),
  );
  policy.messages.allowInsert.where((message) =>
    policy.chatMembers.exists.where({ chatId: message.chatId, userId: session.user_id }),
  );
  policy.messages.allowDelete.where({ senderId: session.user_id });

  policy.reactions.allowRead.where(allowedTo.read("messageId"));
  policy.reactions.allowInsert.where({ userId: session.user_id });
  policy.reactions.allowDelete.where({ userId: session.user_id });

  policy.canvases.allowRead.where((canvas) =>
    anyOf([
      allowedTo.read("chatId"),
      policy.chatMembers.exists.where({ chatId: canvas.chatId, userId: session.user_id }),
    ]),
  );
  policy.canvases.allowInsert.where((canvas) =>
    policy.chatMembers.exists.where({ chatId: canvas.chatId, userId: session.user_id }),
  );

  policy.strokes.allowRead.where(allowedTo.read("canvasId"));
  policy.strokes.allowInsert.where(allowedTo.read("canvasId"));
  policy.strokes.allowDelete.where({ ownerId: session.user_id });

  policy.attachments.allowRead.where(allowedTo.read("messageId"));
  policy.attachments.allowInsert.where(allowedTo.read("messageId"));

  policy.files.allowInsert.where({});

  policy.files.allowRead.where(allowedTo.readReferencing(policy.attachments, "fileId"));
  policy.files.allowDelete.where(allowedTo.deleteReferencing(policy.attachments, "fileId"));
});
