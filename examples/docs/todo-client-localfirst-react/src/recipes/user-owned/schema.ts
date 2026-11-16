import { schema as s } from "jazz-tools";

// #region schema
export const schema = s.defineSchema({
  todos: s.table({ title: s.string(), done: s.boolean() }),
});
// #endregion schema

// #region permissions
const { definePermissions, session } = schema;

const createdByMe = { $createdBy: session.user };

export const permissions = definePermissions({
  todos: {
    read: createdByMe,
    insert: true,
    update: createdByMe,
    delete: createdByMe,
  },
});
// #endregion permissions
