import { schema as s } from "jazz-tools";

// Rust apps use the same TypeScript migration files and push them with the same CLI.
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
      parent: s.ref("todos").optional(),
      project: s.ref("projects").optional(),
    }),
  },
  to: {
    todos: s.table({
      title: s.string(),
      done: s.boolean(),
      description: s.string().optional(),
      parent: s.ref("todos").optional(),
      project: s.ref("projects").optional(),
    }),
  },
});
