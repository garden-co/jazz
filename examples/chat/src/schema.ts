import { co } from "jazz-tools";

export const Message = co
  .map({
    text: co.plainText(),
    image: co.optional(co.image()),
  })
  .withPermissions({
    onInlineCreate: "sameAsContainer",
  });
export type Message = co.loaded<typeof Message>;

export const Chat = co.list(Message).withPermissions({
  onCreate(newGroup) {
    newGroup.makePublic("writer");
  },
});
export type Chat = co.loaded<typeof Chat>;
