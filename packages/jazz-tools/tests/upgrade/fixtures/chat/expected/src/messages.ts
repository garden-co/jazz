import { app, type Profile } from "../jazz.generated.js";
import { type Message } from "../app-extras.js";

export const messagesOf = (chatId: string) => app.messages.where({ chatId });
export const label = (message: Message, sender: Profile) =>
  `${sender.name} at ${message.$createdAt.toISOString()}: ${message.text}`;
