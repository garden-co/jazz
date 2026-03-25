import { table, col } from "jazz-tools";
import { z } from "zod";

table("users", {
  name: col.string(),
  friendsIds: col.array(col.ref("users")).default([]),
});

table("projects", {
  name: col.string(),
});

table("todos", {
  title: col.string(),
  done: col.boolean().default(false),
  tags: col.array(col.string()).default([]),
  projectId: col.ref("projects"),
  ownerId: col.ref("users").optional(),
  assigneesIds: col.array(col.ref("users")).default([]),
});

const jsonSchema = z.object({
  name: z.string(),
  age: z.number().optional(),
});

table("table_with_defaults", {
  integer: col.int().default(1),
  float: col.float().default(1),
  bytes: col.bytes().default(new Uint8Array([0, 1, 255])),
  enum: col.enum("a", "b", "c").default("a"),
  json: col.json(jsonSchema).default({ name: "default name" }),
  timestampDate: col.timestamp().default(new Date("2026-01-01")),
  timestampNumber: col.timestamp().default(0),
  string: col.string().default("default value"),
  array: col.array(col.string()).default(["a", "b", "c"]),
  boolean: col.boolean().default(true),
  nullable: col.string().optional().default(null),
  refId: col.ref("todos").optional().default("00000000-0000-0000-0000-000000000000"),
});
