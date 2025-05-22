import { co, z } from "jazz-tools";

export const Message = co.map({
  text: co.plainText(),
  image: z.optional(co.image()),
});
export type Message = co.loaded<typeof Message>;

export const Chat = co.list(Message);
export type Chat = co.loaded<typeof Chat>;
