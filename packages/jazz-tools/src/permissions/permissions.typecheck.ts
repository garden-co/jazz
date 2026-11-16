// Type tests for the permissions DSL: each `@ts-expect-error` must fire.
import { schema as s } from "../index.js";

const schema = s.defineSchema({
  chats: s.table(
    { name: s.string().optional(), isPublic: s.boolean(), ownerId: s.uuid(), size: s.int() },
    { members: s.reverse("members", "chat") },
  ),
  members: s.table(
    { chatId: s.uuid(), userId: s.uuid(), role: s.enum("admin", "member") },
    { chat: s.rel("chats", "chatId") },
  ),
});
const { definePermissions, exists, allowedTo, session, anyOf, allOf, not, ne, gt, eq, isIn } =
  schema;
const isMember = (chatId: s.Uuid) => exists.members({ chatId, userId: session.user });

// Fine: every form the design names.
definePermissions({
  chats: {
    read: (chat) => anyOf({ isPublic: true }, isMember(chat.id), { name: null }),
    insert: { ownerId: session.user, size: gt(0) },
    updateIf: (chat) => isMember(chat.id),
    updateTo: (chat, old) => allOf(isMember(chat.id), { isPublic: old.isPublic, size: gt(old.size) }),
    delete: (chat) => not(chat.members.where({ role: "admin" })),
  },
  members: {
    read: (m) => anyOf({ userId: session.user }, allowedTo.read(m.chat, { maxDepth: 2 })),
    insert: (m) =>
      allOf(
        m.chat.where({ ownerId: session.user }),
        { role: ["admin", "member"] },
        eq(session.claims.role, "admin"),
      ),
    update: { userId: session.user, role: ne("admin") },
    delete: (m) =>
      allOf(
        exists.chats({ id: m.chatId }, anyOf({ isPublic: true }, { size: 1 })).hop("members", {
          userId: session.user,
        }),
        { chatId: isIn(session.claims.chats) },
        allowedTo.delete(m.chatId),
        allowedTo.delete(schema.tables.members.chatId),
      ),
  },
});

// Unknown column, in a rule's condition object.
// @ts-expect-error unknown column
definePermissions({ chats: { read: { nmae: "x" } }, members: {} });

// Unknown column, inside anyOf at a rule's top.
definePermissions({
  // @ts-expect-error unknown column
  chats: { read: () => anyOf({ isPublic: true }, { nmae: "x" }) },
  members: {},
});

// Unknown column, in exists.
// @ts-expect-error unknown column
exists.members({ chatID: "x" });

// Wrong value type.
// @ts-expect-error wrong value type
definePermissions({ chats: { read: { isPublic: "yes" } }, members: {} });
// @ts-expect-error wrong value type: an int column compared with a uuid
exists.chats({ size: session.user });
// @ts-expect-error wrong value type: not an enum case
exists.members({ role: "owner" });
// @ts-expect-error null for a column that isn't optional
exists.members({ userId: null });

// `update` together with `updateIf`.
definePermissions({
  // @ts-expect-error update with updateIf
  chats: { update: true, updateIf: true, updateTo: true },
  members: {},
});

// `updateIf` without `updateTo`.
// @ts-expect-error updateIf without updateTo
definePermissions({ chats: { updateIf: true }, members: {} });

// A missing table.
// @ts-expect-error missing table members
definePermissions({ chats: {} });

// An unknown table.
// @ts-expect-error unknown table
definePermissions({ chats: {}, members: {}, nope: {} });

// An unknown relation.
// @ts-expect-error unknown relation
definePermissions({ chats: { read: (chat) => chat.memebrs.where({}) }, members: {} });
// @ts-expect-error unknown relation in a hop
exists.members({}).hop("chats");

// An unknown rule key.
// @ts-expect-error unknown rule key
definePermissions({ chats: { select: true }, members: {} });

// `old` outside updateTo's direct comparisons.
// @ts-expect-error old values aren't values for exists
definePermissions({ chats: { updateIf: true, updateTo: (c, old) => exists.chats({ size: old.size }) }, members: {} });
