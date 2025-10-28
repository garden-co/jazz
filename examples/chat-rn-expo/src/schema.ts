import { co, z } from "jazz-tools";

export const Message = co.map({
  text: z.string(),
});
export type Message = co.loaded<typeof Message>;

export const Chat = co.list(Message);
export type Chat = co.loaded<typeof Chat>;
