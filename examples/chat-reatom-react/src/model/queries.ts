import { app } from "../../schema.js";
import { jazz } from "@/jazz";

export const getChatRowsQuery = jazz.reatomCachedQuery(
  (chatId: string) => app.chats.where({ id: chatId }),
  "chatRows",
);

export const getChatMembersQuery = jazz.reatomCachedQuery(
  (chatId: string) => app.chatMembers.where({ chatId }),
  "chatMembers",
);

export const getMessageAttachmentsQuery = jazz.reatomCachedQuery(
  (messageId: string) => app.attachments.where({ messageId }),
  "msgAttachments",
);

export const getMessageReactionsQuery = jazz.reatomCachedQuery(
  (messageId: string) => app.reactions.where({ messageId }),
  "msgReactions",
);

export const getCanvasRowsQuery = jazz.reatomCachedQuery(
  (canvasId: string) => app.canvases.where({ id: canvasId }),
  "canvasRows",
);

export const getCanvasStrokesQuery = jazz.reatomCachedQuery(
  (canvasId: string) => app.strokes.where({ canvasId }),
  "canvasStrokes",
);

export const getSenderProfileQuery = jazz.reatomCachedQuery(
  (senderId: string) => app.profiles.where({ id: senderId }),
  "senderProfile",
);

export const getProfilesByUserIdQuery = jazz.reatomCachedQuery(
  (userId: string) => app.profiles.where({ userId }),
  "profilesByUserId",
);

export const getFirstChatMessageQuery = jazz.reatomCachedQuery(
  (chatId: string) => app.messages.where({ chatId }).orderBy("createdAt", "asc").limit(1),
  "firstChatMessage",
);

export const getChatMessagesQuery = jazz.reatomCachedQuery(
  (chatId: string, limit: number) =>
    app.messages
      .where({ chatId })
      .include({ sender: true })
      .orderBy("createdAt", "desc")
      .limit(limit),
  "chatMessages",
);

export const allProfilesQuery = jazz.reatomQueryAll(() => app.profiles, "allProfiles");

export const myMembershipsQuery = jazz.reatomQueryAll(() => {
  const userId = jazz().session?.user_id ?? "__none__";
  return app.chatMembers.where({ userId }).include({ chat: true });
}, "myMemberships");

export const myMembershipForChatQuery = jazz.reatomCachedQuery((chatId: string) => {
  const userId = jazz().session?.user_id ?? "__none__";
  return app.chatMembers.where({ chatId, userId });
}, "myMembershipForChat");
