import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  invitations: s.table({ subject: s.uuid(), label: s.string() }),
});

const { definePermissions, session, anyOf } = schema;

export const permissions = definePermissions({
  invitations: {
    read: anyOf({ subject: session.user }, { label: "unmatched control branch" }),
    insert: true,
  },
});
