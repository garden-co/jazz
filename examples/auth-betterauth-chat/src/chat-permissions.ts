import type { Db, TableProxy } from "jazz-tools";

export type ChatMessageInput = {
  author_name: string;
  chat_id: string;
  text: string;
  sent_at: Date;
};

export function chatMessageInput(
  chatId: string,
  authorName: string,
  text: string,
): ChatMessageInput {
  return {
    author_name: authorName,
    chat_id: chatId,
    text,
    sent_at: new Date(),
  };
}

export async function canInsertChatMessage<T, Init>(
  db: Pick<Db, "canInsert">,
  messages: TableProxy<T, Init>,
  message: Init,
): Promise<boolean> {
  return (await db.canInsert(messages, message)) === true;
}
