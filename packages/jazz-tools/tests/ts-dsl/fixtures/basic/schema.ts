import { TypedTableQueryBuilder, schema as s } from "../../../../src/index.js";
import { schemaToWasm } from "../../../../src/codegen/schema-reader.js";
import { schemaDefinitionToAst } from "../../../../src/migrations.js";
import type { CompiledPermissions } from "../../../../src/permissions/index.js";
import { mergePermissionsIntoSchema } from "../../../../src/schema-permissions.js";

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
      done: s.boolean(),
      tags: s.array(s.string()),
      projectId: s.ref("projects"),
      ownerId: s.ref("users").optional(),
      assigneesIds: s.array(s.ref("users")),
    })
    .index("by_done", ["done"]),
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
