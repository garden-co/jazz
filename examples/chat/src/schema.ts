import { co } from "jazz-tools";

export const Message = co.map({
  text: co.plainText(),
  image: co.optional(co.image()),
});
export type Message = co.loaded<typeof Message>;

export const Chat = co.list(Message);
export type Chat = co.loaded<typeof Chat>;
