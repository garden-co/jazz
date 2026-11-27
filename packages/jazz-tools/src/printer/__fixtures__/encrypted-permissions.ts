import * as s from "../../schema-builders.js";

export const schema = s.defineSchema({
  rooms: s.table({ name: s.string() }, {}, { space: "id" }),
  messages: s.table(
    { roomId: s.uuid(), authorId: s.uuid(), text: s.string().encrypted() },
    { room: s.rel("rooms", "roomId") },
    { space: "room" },
  ),
});

const { allOf, definePermissions, isMember, session } = schema;

export const permissions = definePermissions({
  rooms: { read: (room) => isMember("rooms", room.id) },
  messages: {
    read: (message) => allOf(isMember("rooms", message.roomId), { authorId: session.user }),
  },
});
