import { app } from "../schema.js";
import { account, db } from "./client.js";

export async function createChat() {
  // Wait for the space to be accepted before inserting protected messages.
  return db.insert(app.chats, { ownerId: account.id }).wait({ tier: "global" });
}

export async function sendMessage(chatId: string, text: string) {
  return db.insert(app.messages, { chatId, text }).wait({ tier: "global" });
}

export async function readMessages(chatId: string) {
  return db.all(app.messages.where({ chatId }).orderBy("$createdAt", "asc"), { tier: "remote" });
}

export async function shareChat(chatId: string, recipientAccountId: string) {
  await db.e2ee.spaces.grant(app.chats, chatId, recipientAccountId).wait();
}
