import { definePermissions } from "jazz-tools/permissions";
import { app } from "./app.js";

export default definePermissions(app, ({ policy, session, anyOf, allowedTo }) => {
  // Profiles: anyone reads, only owner writes
  policy.profiles.allowRead.where({});
  policy.profiles.allowInsert.where({ userId: session.user_id });
  policy.profiles.allowUpdate.where({ userId: session.user_id });

  // Chats: readable if public, or by chat members.
  policy.chats.allowRead.where((chat) =>
    anyOf([
      { isPublic: true },
      policy.chatMembers.exists.where({ chat: chat.id, userId: session.user_id }),
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
      allowedTo.read("chat"),
      policy.chatMembers.exists.where({ chat: message.chat, userId: session.user_id }),
    ]),
  );
  policy.messages.allowInsert.where((message) =>
    policy.chatMembers.exists.where({ chat: message.chat, userId: session.user_id }),
  );
  policy.messages.allowDelete.where({ senderId: session.user_id });

  // Reactions: inherit read from message; owner inserts/deletes
  policy.reactions.allowRead.where(allowedTo.read("message"));
  policy.reactions.allowInsert.where({ userId: session.user_id });
  policy.reactions.allowDelete.where({ userId: session.user_id });

  // Canvases: same pattern as messages.
  policy.canvases.allowRead.where((canvas) =>
    anyOf([
      allowedTo.read("chat"),
      policy.chatMembers.exists.where({ chat: canvas.chat, userId: session.user_id }),
    ]),
  );
  policy.canvases.allowInsert.where((canvas) =>
    policy.chatMembers.exists.where({ chat: canvas.chat, userId: session.user_id }),
  );

  // Strokes: inherit from canvas; owner deletes
  policy.strokes.allowRead.where(allowedTo.read("canvas"));
  policy.strokes.allowInsert.where(allowedTo.read("canvas"));
  policy.strokes.allowDelete.where({ ownerId: session.user_id });

  // Attachments: inherit read from message; chat members can insert
  policy.attachments.allowRead.where(allowedTo.read("message"));
  policy.attachments.allowInsert.where(allowedTo.read("message"));
});
