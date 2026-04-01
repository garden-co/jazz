import { schema as s } from "jazz-tools";

export default s.defineMigration({
  migrate: {
    todos: {
      description: s.add.string({ default: null }),
    },
  },
  fromHash: "a01f5c72ec47",
  toHash: "311995e9a178",
  from: {
    todos: s.table({
      title: s.string(),
      done: s.boolean(),
      parentId: s.ref("todos").optional(),
      projectId: s.ref("projects").optional(),
      owner_id: s.string(),
    }),
  },
  to: {
    todos: s.table({
      title: s.string(),
      done: s.boolean(),
      description: s.string().optional(),
      parentId: s.ref("todos").optional(),
      projectId: s.ref("projects").optional(),
      owner_id: s.string(),
    }),
  },
});
