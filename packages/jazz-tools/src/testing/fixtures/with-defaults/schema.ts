import { schema as s } from "../../../schema-namespace.js";
import { defineApp } from "../../../typed-app.js";

export const app = defineApp({
  todos: {
    title: s.string(),
    done: s.boolean().default(false),
    tags: s.array(s.string()).default(["work", "home"]),
    metadata: s.json().default({ createdBy: "alice" }),
    avatar: s.bytes().default(new Uint8Array([0, 1, 255])),
  },
  counters: {
    count: s.int().merge("counter") as ReturnType<typeof s.int>,
  },
});
