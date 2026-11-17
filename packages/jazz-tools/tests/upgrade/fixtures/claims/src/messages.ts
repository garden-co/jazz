import { app } from "../schema";
import { CHAT_ID } from "../constants";

export const chatMessages = app.messages.where({ chat_id: CHAT_ID });
