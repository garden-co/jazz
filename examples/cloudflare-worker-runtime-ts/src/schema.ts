import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  todos: s.table({ title: s.string(), done: s.boolean() }, {}),
});

const { definePermissions } = schema;

export const permissions = definePermissions({
  todos: {},
});
