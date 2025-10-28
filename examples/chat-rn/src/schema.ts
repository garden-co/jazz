import { co } from "jazz-tools";

export const Message = co.map({
  text: co.plainText(),
});

export const Chat = co.list(Message);
