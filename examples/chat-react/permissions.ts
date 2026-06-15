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
      allowedTo.read("chatId"),
      policy.chatMembers.exists.where({ chatId: message.chatId, userId: session.user_id }),
    ]),
  );
  policy.messages.allowInsert.where((message) =>
    policy.chatMembers.exists.where({ chatId: message.chatId, userId: session.user_id }),
  );
  // `senderId` references a profile row, so the message is the sender's iff a
  // profile with that id belongs to the session user. (Comparing `senderId`
  // directly to `session.user_id` never matched — a profile id is not a user id.)
  policy.messages.allowDelete.where((message) =>
    policy.profiles.exists.where({ id: message.senderId, userId: session.user_id }),
  );

  policy.reactions.allowRead.where(allowedTo.read("messageId"));
  policy.reactions.allowInsert.where({ userId: session.user_id });
  // The reactor may always remove their own reaction; the message's sender may
  // also clear reactions when deleting the message they hang off.
  policy.reactions.allowDelete.where(
    anyOf([{ userId: session.user_id }, allowedTo.delete("messageId")]),
  );

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
  // An attachment is deletable by whoever may delete its message; files and
  // file_parts then cascade via the deleteReferencing policies below.
  policy.attachments.allowDelete.where(allowedTo.delete("messageId"));

  policy.files.allowInsert.where({});
  policy.file_parts.allowInsert.where({});

  policy.files.allowRead.where(allowedTo.readReferencing(policy.attachments, "fileId"));
  policy.file_parts.allowRead.where(allowedTo.readReferencing(policy.files, "partIds"));
  policy.files.allowDelete.where(allowedTo.deleteReferencing(policy.attachments, "fileId"));
  policy.file_parts.allowDelete.where(allowedTo.deleteReferencing(policy.files, "partIds"));
});
