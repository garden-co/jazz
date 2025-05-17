import { co, z } from "jazz-tools";

export const Message = co.map({
  text: co.plainText(),
  image: z.optional(co.image()),
});

export const Chat = co.list(Message);
