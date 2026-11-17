import { app, type Message, type Profile } from "../schema.js";

export const messagesOf = (chatId: string) => app.messages.where({ chatId });
export const label = (message: Message, sender: Profile) =>
  `${sender.name} at ${message.$createdAt.toISOString()}: ${message.text}`;
