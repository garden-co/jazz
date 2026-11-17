import { app } from "../jazz.generated";
import { CHAT_ID } from "../constants";

export const chatMessages = app.messages.where({ chat_id: CHAT_ID });
