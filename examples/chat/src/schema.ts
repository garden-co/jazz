import { co, setDefaultValidationMode } from "jazz-tools";

setDefaultValidationMode("strict");

const BaseMessage = co.map({
  text: co.plainText(),
  image: co.optional(co.image()),
  get replyOf() {
    return co.optional(MessageSnapshot);
  },
});

export const Message = BaseMessage.resolved({
  text: true,
  image: true,
  replyOf: { ref: { text: true } },
}).withPermissions({
  onInlineCreate: "sameAsContainer",
});

export type Message = co.loaded<typeof Message>;

export const MessageSnapshot: co.Snapshot<
  typeof BaseMessage,
  { text: true },
  { ref: { text: true } }
> = co
  .snapshotRef(BaseMessage, { cursorResolve: { text: true } })
  .resolved({ ref: { text: true } });

export type MessageSnapshot = co.loaded<typeof MessageSnapshot>;

export const Chat = co.list(Message).withPermissions({
  onCreate: (owner) => owner.addMember("everyone", "writer"),
});
export type Chat = co.loaded<typeof Chat>;
