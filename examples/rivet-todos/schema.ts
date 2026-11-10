import { schema as s } from "jazz-tools";

export const app = s.defineApp(
  {
    todos: s.table({ title: s.string(), done: s.boolean(), owner_id: s.uuid() }, {}),
  },
  { migrations: "none" },
);
