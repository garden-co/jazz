// #region schema
import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  tasks: s.table({
    title: s.string(),
    score: s.int(),
    priority: s.computed(s.enum("low", "medium", "high"), ["score"]),
  }),
});
// #endregion schema

// #region permissions
const { definePermissions } = schema;

export const permissions = definePermissions({
  tasks: {
    read: true,
    insert: true,
    update: true,
    delete: true,
  },
});
// #endregion permissions
