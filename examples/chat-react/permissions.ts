import { definePermissions, RowContext } from "jazz-tools/permissions";
import { app, Chat } from "./schema.js";

export default definePermissions(app, ({ policy, session, allOf, anyOf, allowedTo }) => {
  policy.profiles.allowRead.where({});
  policy.profiles.allowInsert.where({ userId: session.user.account });
  policy.profiles.allowUpdate.where({ userId: session.user.account });

  const userIsChatMember = (chat: RowContext<Chat>) =>
    policy.chatMembers.exists.where({ chatId: chat.id, userId: session.user.account });
  policy.chats.allowRead.where((chat) =>
    anyOf([{ isPublic: true }, userIsChatMember(chat), { joinCode: session.claims["join_code"] }]),
  );
  policy.chats.allowInsert.where({ "$createdBy.account": session.user.account });
  policy.chats.allowUpdate.whereOld(userIsChatMember).whereNew((chat) =>
    allOf([
      userIsChatMember(chat),
      // Users may update only non-protected fields. `isPublic` cannot be updated.
      policy.chats.exists.where({
        id: chat.id,
        isPublic: chat.isPublic,
      }),
    ]),
  );

  policy.chatMembers.allowRead.where((member) =>
    anyOf([
      { userId: session.user.account },
      policy.chatMembers.exists.where({ chatId: member.chatId, userId: session.user.account }),
    ]),
  );
  policy.chatMembers.allowInsert.where((member) =>
    anyOf([
      allOf([
        { userId: session.user.account },
        policy.chats.exists.where({ id: member.chatId, isPublic: true }),
      ]),
      allOf([
        { userId: session.user.account },
        policy.chats.exists.where({
          id: member.chatId,
          "$createdBy.account": session.user.account,
        }),
      ]),
      allOf([
        { userId: session.user.account },
        { joinCode: { ne: null } },
        policy.chats.exists.where({ id: member.chatId, joinCode: member.joinCode }),
      ]),
    ]),
  );
  policy.chatMembers.allowDelete.where({ userId: session.user.account });

  policy.messages.allowRead.where((message) =>
    anyOf([
      policy.chats.exists.where({ id: message.chatId, isPublic: true }),
      policy.chatMembers.exists.where({ chatId: message.chatId, userId: session.user.account }),
    ]),
  );
  policy.messages.allowInsert.where((message) =>
    allOf([
      policy.chatMembers.exists.where({ chatId: message.chatId, userId: session.user.account }),
      policy.profiles.exists.where({ id: message.senderId, userId: session.user.account }),
    ]),
  );
  policy.messages.allowDelete.where((message) =>
    policy.profiles.exists.where({ id: message.senderId, userId: session.user.account }),
  );

  policy.reactions.allowRead.where(allowedTo.read("messageId"));
  policy.reactions.allowInsert.where({ userId: session.user.account });
  policy.reactions.allowDelete.where({ userId: session.user.account });

  policy.canvases.allowRead.where(allowedTo.read("chatId"));
  policy.canvases.allowInsert.where((canvas) =>
    policy.chatMembers.exists.where({ chatId: canvas.chatId, userId: session.user.account }),
  );

  policy.strokes.allowRead.where(allowedTo.read("canvasId"));
  policy.strokes.allowInsert.where(allowedTo.read("canvasId"));
  policy.strokes.allowDelete.where({ "$createdBy.account": session.user.account });
});
