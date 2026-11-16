// #region schema
import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  todos: s.table({ title: s.string(), done: s.boolean(), owner_id: s.uuid() }),
});
// #endregion schema

// #region permissions
const { definePermissions, session } = schema;

const ownedByMe = { owner_id: session.user };

export const permissions = definePermissions({
  todos: {
    read: ownedByMe,
    insert: true,
    update: ownedByMe,
    delete: ownedByMe,
  },
});
// #endregion permissions
