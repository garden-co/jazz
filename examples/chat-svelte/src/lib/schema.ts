import { co } from 'jazz-tools';

export const Message = co.map({
  text: co.plainText(),
  image: co.optional(co.image())
});

export const Chat = co.list(Message);
