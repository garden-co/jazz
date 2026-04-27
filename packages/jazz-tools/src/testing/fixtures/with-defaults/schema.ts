import { col } from "../../../dsl.js";
import { defineApp } from "../../../typed-app.js";

export const app = defineApp({
  todos: {
    title: col.string(),
    done: col.boolean().default(false),
    tags: col.array(col.string()).default(["work", "home"]),
    metadata: col.json().default({ createdBy: "alice" }),
    avatar: col.bytes().default(new Uint8Array([0, 1, 255])),
  },
  counters: {
    count: col.int().merge("counter") as ReturnType<typeof col.int>,
  },
});
