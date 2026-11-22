import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  notes: s.table({ title: s.string(), owner: s.uuid() }, {}),
});

const { definePermissions, session } = schema;
const mine = { owner: session.user };

export const permissions = definePermissions({
  notes: { read: mine, insert: mine, update: mine, delete: mine },
});
