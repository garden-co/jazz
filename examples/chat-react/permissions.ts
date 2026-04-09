import { definePermissions } from "jazz-tools/permissions";
import { app } from "./schema.js";

export default definePermissions(app, ({ policy, session, anyOf, allowedTo }) => {
  policy.profiles.allowRead.where({});
  policy.profiles.allowInsert.where({ userId: session.user_id });
  policy.profiles.allowUpdate.where({ userId: session.user_id });
  policy.profiles.allowDelete.never();

  policy.chats.allowRead.where((chat) =>
    anyOf([
      { isPublic: true },
      policy.chatMembers.exists.where({ chatId: chat.id, userId: session.user_id }),
      { joinCode: session["claims.join_code"] },
    ]),
  );
  policy.chats.allowInsert.where({ createdBy: session.user_id });
  policy.chats.allowUpdate.where((chat) =>
    policy.chatMembers.exists.where({ chatId: chat.id, userId: session.user_id }),
  );
  policy.chats.allowDelete.never();

  policy.chatMembers.allowRead.where((member) =>
    anyOf([
      { userId: session.user_id },
      policy.chatMembers.exists.where({ chatId: member.chatId, userId: session.user_id }),
    ]),
  );
  policy.chatMembers.allowInsert.where({ userId: session.user_id });
  policy.chatMembers.allowUpdate.never();
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
  policy.messages.allowUpdate.never();
  policy.messages.allowDelete.where({ senderId: session.user_id });

  policy.reactions.allowRead.where(allowedTo.read("messageId"));
  policy.reactions.allowInsert.where({ userId: session.user_id });
  policy.reactions.allowUpdate.never();
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
  policy.canvases.allowUpdate.never();
  policy.canvases.allowDelete.never();

  policy.strokes.allowRead.where(allowedTo.read("canvasId"));
  policy.strokes.allowInsert.where(allowedTo.read("canvasId"));
  policy.strokes.allowUpdate.never();
  policy.strokes.allowDelete.where({ ownerId: session.user_id });

  policy.attachments.allowRead.where(allowedTo.read("messageId"));
  policy.attachments.allowInsert.where(allowedTo.read("messageId"));
  policy.attachments.allowUpdate.never();
  policy.attachments.allowDelete.never();

  policy.files.allowInsert.where({});
  policy.file_parts.allowInsert.where({});
  policy.files.allowUpdate.never();
  policy.file_parts.allowUpdate.never();

  policy.files.allowRead.where(allowedTo.readReferencing(policy.attachments, "fileId"));
  policy.file_parts.allowRead.where(allowedTo.readReferencing(policy.files, "partIds"));
  policy.files.allowDelete.where(allowedTo.deleteReferencing(policy.attachments, "fileId"));
  policy.file_parts.allowDelete.where(allowedTo.deleteReferencing(policy.files, "partIds"));
});
