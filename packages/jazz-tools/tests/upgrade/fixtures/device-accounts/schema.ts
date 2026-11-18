import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  notes: s.table({ owner: s.uuid(), text: s.string() }),
});

const { definePermissions, session, anyOf, allOf, eq, ne, isIn } = schema;

// Device accounts read everything; everyone else only their own notes.
const deviceAccount = eq(session.authMode, "local-first");

export const permissions = definePermissions({
  notes: {
    read: anyOf({ owner: session.user }, deviceAccount),
    insert: allOf({ owner: session.user }, ne(session.authMode, "local-first")),
    update: allOf({ owner: session.user }, isIn(session.authMode, ["external", "local-first"])),
    delete: { owner: session.user },
  },
});
