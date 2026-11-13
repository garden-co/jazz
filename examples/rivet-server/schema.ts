import { schema as s } from "jazz-tools";

// Clients import this same application schema.
export const app = s.defineApp(
  {
    todos: s.table({ title: s.string(), done: s.boolean(), owner_id: s.uuid() }, {}),
  },
  { migrations: "none" },
);
