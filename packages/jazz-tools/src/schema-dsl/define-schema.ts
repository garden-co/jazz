/**
 * `s.defineSchema(tables)`: the app's schema. Its tables are under
 * `.tables` (each also exposing its columns as handles, for
 * `allowedTo.<op>(schema.tables.t.col)`), and its permission tools are its
 * own properties:
 *
 * ```ts
 * export const schema = s.defineSchema({ … });
 * const { definePermissions, exists, allowedTo, session, anyOf } = schema;
 * export const permissions = definePermissions({ … });
 * ```
 *
 * `jazz-tools generate` reads the exports `schema` and `permissions` and
 * writes `jazz.app.json` and `jazz.permissions.json` through the Rust
 * writer. Nothing imports `schema.ts` at run time.
 */
import type { AnyColumnDefinition } from "../dsl.js";
import { isComputedColumn } from "../dsl.js";
import type { Column } from "../schema.js";
import { handle, type ColumnMeta, type SchemaMeta, type TableMeta } from "../permissions/compile.js";
import { permissionTools, type PermissionTools } from "../permissions/dsl.js";
import type { AnyClaims, ClaimsMarker, SchemaTables, TableHandles, TypedClaims } from "../permissions/types.js";
import { readableApp, type ReadableApp } from "./readable-app.js";
import { isDefinedTable, type DefinedTable } from "./table.js";
import { assertSchemaNameAllowed } from "../schema-name.js";

export const SCHEMA_BRAND: unique symbol = Symbol.for("jazz.schema") as never;

/** What `s.defineSchema` returns. */
export type Schema<S extends SchemaTables = SchemaTables, Claims = AnyClaims> = PermissionTools<
  S,
  Claims
> & {
  /** The tables, by name. `schema.tables.t.col` is a column handle (for `allowedTo` `via`). */
  readonly tables: TableHandles<S>;
  /** @internal */
  readonly [SCHEMA_BRAND]: { readonly app: ReadableApp };
  /** @internal The tables' types (types only). */
  readonly __tables?: S;
};

/** Options of `s.defineSchema`. */
export interface DefineSchemaOptions<C = unknown> {
  /** The session claims' type: `s.claims<{ join_code: string }>()`. */
  claims?: ClaimsMarker<C>;
}

/** `s.claims<T>()`: types `session.claims` as `T`. */
export function claims<C>(): ClaimsMarker<C> {
  return {};
}

function tableMetaOf(table: DefinedTable): TableMeta {
  const columns = new Map<string, ColumnMeta>();
  for (const [name, definition] of Object.entries(
    table.columns as Record<string, AnyColumnDefinition>,
  )) {
    if (isComputedColumn(definition)) continue;
    const column = (definition as unknown as { _build(name: string): Column })._build(name);
    columns.set(name, { sqlType: column.sqlType, nullable: column.nullable });
  }
  return { columns, relations: table.relations };
}

function checkRelations(tables: Record<string, DefinedTable>): void {
  for (const [tableName, table] of Object.entries(tables)) {
    for (const [relationName, relation] of Object.entries(table.relations)) {
      const target = tables[relation.table];
      if (!target)
        throw new Error(
          `${tableName}.${relationName}: relation to unknown table "${relation.table}".`,
        );
      if (relation.kind === "reverse") {
        const forward = target.relations[relation.relation];
        if (!forward || forward.kind !== "forward" || forward.table !== tableName)
          throw new Error(
            `${tableName}.${relationName}: s.reverse("${relation.table}", "${relation.relation}") needs ${relation.table}.${relation.relation} to be s.rel("${tableName}", …).`,
          );
      }
    }
  }
}

/**
 * Define the app's schema from its tables (`s.table(...)`). The result
 * holds the tables under `.tables` and the permission tools
 * (`definePermissions`, `exists`, `allowedTo`, `session`, …).
 */
export function defineSchema<const S extends Record<string, DefinedTable<any, any>>, C = unknown>(
  tables: S,
  options: DefineSchemaOptions<C> = {},
): Schema<S, unknown extends C ? AnyClaims : TypedClaims<C>> {
  void options;
  if (!tables || typeof tables !== "object" || Array.isArray(tables))
    throw new Error("s.defineSchema(tables) takes an object of s.table(...) definitions.");
  const meta = new Map<string, TableMeta>();
  for (const [name, table] of Object.entries(tables)) {
    assertSchemaNameAllowed(name);
    if (!isDefinedTable(table))
      throw new Error(`s.defineSchema: "${name}" is not a table; define it with s.table(columns, relations).`);
    meta.set(name, tableMetaOf(table));
  }
  checkRelations(tables);
  const app = readableApp(tables);
  const handles: Record<string, unknown> = {};
  for (const [name, table] of Object.entries(tables)) {
    const view: Record<string | symbol, unknown> = Object.create(table) as Record<string, unknown>;
    for (const column of meta.get(name)!.columns.keys()) {
      if (column in table) continue;
      Object.defineProperty(view, column, {
        enumerable: false,
        value: handle({ k: "tableColumn", table: name, column }),
      });
    }
    handles[name] = Object.freeze(view);
  }
  const schemaMeta: SchemaMeta = meta;
  return Object.freeze({
    ...permissionTools(schemaMeta),
    tables: Object.freeze(handles),
    [SCHEMA_BRAND]: { app },
  }) as unknown as Schema<S, unknown extends C ? AnyClaims : TypedClaims<C>>;
}

/** The readable `jazz.app.json` parts (`tables`, `transforms`) of a schema, or undefined if it isn't one. */
export function readableSchema(value: unknown): ReadableApp | undefined {
  if (typeof value !== "object" || value === null) return undefined;
  return (value as { [SCHEMA_BRAND]?: { app: ReadableApp } })[SCHEMA_BRAND]?.app;
}
