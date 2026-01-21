import { co, z, setDefaultValidationMode } from "jazz-tools";

setDefaultValidationMode("strict");

export const Message = co.map({
  text: co.plainText(),
  image: co.image().optional(),
});
export type Message = co.loaded<typeof Message>;

export const Chat = co.list(Message);
export type Chat = co.loaded<typeof Chat>;
