import { schema as s } from "jazz-tools";
export const schema = s.defineSchema({
  notes: s.table({ text: s.string(), owner_id: s.uuid(), photo: s.bytes().optional() }, {}),
});

const { definePermissions, session } = schema;

const ownerIdIsMe = { owner_id: session.user };

export const permissions = definePermissions({
  notes: {
    read: ownerIdIsMe,
    insert: ownerIdIsMe,
    update: ownerIdIsMe,
    delete: ownerIdIsMe,
  },
});
