import { col, defineMigration } from "jazz-tools";

// Rust apps use the same TypeScript migration files and push them with the same CLI.
export default defineMigration({
  migrate: {
    todos: {
      description: col.add.string({ default: null }),
    },
  },
  fromHash: "a01f5c72ec47",
  toHash: "311995e9a178",
  from: {
    todos: {
      title: col.string(),
      done: col.boolean(),
      parent: col.ref("todos").optional(),
      project: col.ref("projects").optional(),
    },
  },
  to: {
    todos: {
      title: col.string(),
      done: col.boolean(),
      description: col.string().optional(),
      parent: col.ref("todos").optional(),
      project: col.ref("projects").optional(),
    },
  },
});
