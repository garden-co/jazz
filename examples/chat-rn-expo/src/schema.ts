import { co, z } from "jazz-tools";
import { ImageDefinition } from "jazz-tools";

export const Message = co.map({
  text: z.string(),
});
export type Message = co.loaded<typeof Message>;

export const Chat = co.list(Message);
export type Chat = co.loaded<typeof Chat>;

export const ChatAccount = co.account({
  root: co.map({}),
  profile: co.profile({
    backgroundPhoto: z.optional(ImageDefinition),
  }),
});
export type ChatAccount = co.loaded<typeof ChatAccount>;
