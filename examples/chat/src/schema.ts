import { ImageDef, j } from "jazz-tools/src/implementation/schema2/schema2.ts";

export const Message = j.CoMap({
  /** @description The text of the message */
  text: j.string(),
  image: ImageDef.optional(),
});

export const Chat = j.CoMap({
  messages: j.CoList(Message),
});

const chat = await Chat.load("123");
