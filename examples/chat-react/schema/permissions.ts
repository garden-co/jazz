import { definePermissions } from "jazz-tools/permissions";
import { app } from "./app.js";

export default definePermissions(app, ({ policy, session, anyOf, allowedTo }) => {
  // Profiles: anyone reads, only owner writes
  policy.profiles.allowRead.where({});
  policy.profiles.allowInsert.where({ userId: session.user_id });
  policy.profiles.allowUpdate.where({ userId: session.user_id });

  // Chats: readable if public, by chat members, or by anyone presenting a
  // valid join code (pre-authorises reading before the chatMember is inserted).
  // Once inserted, no need to keep the join code claim
  policy.chats.allowRead.where((chat) =>
    anyOf([
      { isPublic: true },
      policy.chatMembers.exists.where({ chatId: chat.id, userId: session.user_id }),
      { joinCode: session["claims.join_code"] },
    ]),
  );
  policy.chats.allowInsert.where({ createdBy: session.user_id });

  // Chat members: read own memberships; can only add yourself
  policy.chatMembers.allowRead.where({ userId: session.user_id });
  policy.chatMembers.allowInsert.where({ userId: session.user_id });

  // Messages: inherit read from chat (handles public-chat sync ordering), plus
  // explicit chatMember check as a fallback for private chats in local mode.
  policy.messages.allowRead.where((message) =>
    anyOf([
      allowedTo.read("chatId"),
      policy.chatMembers.exists.where({ chatId: message.chatId, userId: session.user_id }),
    ]),
  );
  policy.messages.allowInsert.where((message) =>
    policy.chatMembers.exists.where({ chatId: message.chatId, userId: session.user_id }),
  );
  policy.messages.allowDelete.where({ senderId: session.user_id });

  // Reactions: inherit read from message; owner inserts/deletes
  policy.reactions.allowRead.where(allowedTo.read("messageId"));
  policy.reactions.allowInsert.where({ userId: session.user_id });
  policy.reactions.allowDelete.where({ userId: session.user_id });

  // Canvases: same pattern as messages.
  policy.canvases.allowRead.where((canvas) =>
    anyOf([
      allowedTo.read("chatId"),
      policy.chatMembers.exists.where({ chatId: canvas.chatId, userId: session.user_id }),
    ]),
  );
  policy.canvases.allowInsert.where((canvas) =>
    policy.chatMembers.exists.where({ chatId: canvas.chatId, userId: session.user_id }),
  );

  // Strokes: inherit from canvas; owner deletes
  policy.strokes.allowRead.where(allowedTo.read("canvasId"));
  policy.strokes.allowInsert.where(allowedTo.read("canvasId"));
  policy.strokes.allowDelete.where({ ownerId: session.user_id });

  // Attachments: inherit read from message; chat members can insert
  policy.attachments.allowRead.where(allowedTo.read("messageId"));
  policy.attachments.allowInsert.where(allowedTo.read("messageId"));

  // Files are created before the parent attachment row exists.
  policy.files.allowInsert.where({});
  policy.file_parts.allowInsert.where({});

  // Files: inherit read and delete inherit from attachments
  policy.files.allowRead.where(allowedTo.readReferencing(policy.attachments, "fileId"));
  policy.file_parts.allowRead.where(allowedTo.readReferencing(policy.files, "partIds"));
  policy.files.allowDelete.where(allowedTo.deleteReferencing(policy.attachments, "fileId"));
  policy.file_parts.allowDelete.where(allowedTo.deleteReferencing(policy.files, "partIds"));
});
