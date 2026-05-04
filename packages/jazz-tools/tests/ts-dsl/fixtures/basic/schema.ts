import { TypedTableQueryBuilder, schema as s } from "../../../../src/index.js";
import { schemaToWasm } from "../../../../src/codegen/schema-reader.js";
import { schemaDefinitionToAst } from "../../../../src/migrations.js";
import type { CompiledPermissions } from "../../../../src/permissions/index.js";
import { mergePermissionsIntoSchema } from "../../../../src/schema-permissions.js";
import { z } from "zod";

const jsonSchema = z.object({
  name: z.string(),
  age: z.number().optional(),
});

export const schema = {
  users: s.table({
    name: s.string(),
    friendsIds: s.array(s.ref("users")),
  }),
  projects: s.table({
    name: s.string(),
  }),
  todos: s
    .table({
      title: s.string(),
      done: s.boolean().default(false),
      tags: s.array(s.string()).default([]),
      projectId: s.ref("projects"),
      ownerId: s.ref("users").optional(),
      assigneesIds: s.array(s.ref("users")).default([]),
    })
    .indexOnly(["done"]),
  table_with_defaults: s.table({
    integer: s.int().default(1),
    float: s.float().default(1),
    bytes: s.bytes().default(new Uint8Array([0, 1, 255])),
    enum: s.enum("a", "b", "c").default("a"),
    json: s.json(jsonSchema).default({ name: "default name" }),
    timestampDate: s.timestamp().default(new Date("2026-01-01")),
    timestampNumber: s.timestamp().default(0),
    string: s.string().default("default value"),
    array: s.array(s.string()).default(["a", "b", "c"]),
    boolean: s.boolean().default(true),
    nullable: s.string().optional().default(null),
    nullableInteger: s.int().optional().default(null),
    refId: s.ref("todos").optional().default("00000000-0000-0000-0000-000000000000"),
  }),
};

export type AppSchema = s.Schema<typeof schema>;
export const baseApp: s.App<AppSchema> = s.defineApp(schema);

export const permissions = s.definePermissions(baseApp, ({ policy }) => {
  policy.todos.allowRead.where({});
  policy.todos.allowInsert.where({});
  policy.todos.allowUpdate.whereOld({ done: false }).whereNew({});
  policy.todos.allowDelete.where({ done: false });
});

function applyPermissions(permissions: CompiledPermissions): s.App<AppSchema> {
  const wasmSchema = schemaToWasm(
    mergePermissionsIntoSchema(schemaDefinitionToAst(schema), permissions),
  );
  const tables = {} as Record<string, TypedTableQueryBuilder<any>>;

  for (const tableName of Object.keys(schema)) {
    tables[tableName] = new TypedTableQueryBuilder(tableName, wasmSchema);
  }

  return {
    ...tables,
    wasmSchema,
  } as s.App<AppSchema>;
}

export const app: s.App<AppSchema> = applyPermissions(permissions);

export type User = s.RowOf<typeof app.users>;
export type Project = s.RowOf<typeof app.projects>;
export type Todo = s.RowOf<typeof app.todos>;
