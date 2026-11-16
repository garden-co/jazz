/**
 * The members of the `schema` namespace (`schema-namespace.ts`): the TS
 * authoring DSL `jazz-tools generate` reads. Apps write `schema.ts` with
 * these; runtime code imports `./jazz.generated` instead.
 *
 * Each builder is its own export so bundlers can drop what an app never
 * names.
 */
import { schemaColumns } from "./dsl.js";
import type { Schema as DefinedSchema } from "./schema-dsl/define-schema.js";
import type {
  Row as RowOf,
  OldRow as OldRowOf,
  SchemaTables,
  Conditions as ConditionsOf,
} from "./permissions/types.js";

export { rel, reverse } from "./relationships.js";
export { allowExternalProvenanceName, computed } from "./dsl.js";
export { defineTable as table } from "./schema-dsl/table.js";
export { defineSchema, claims } from "./schema-dsl/define-schema.js";

export const string = schemaColumns.string;
export const uuid = schemaColumns.uuid;
export const boolean = schemaColumns.boolean;
export const int = schemaColumns.int;
export const bigint = schemaColumns.bigint;
export const timestamp = schemaColumns.timestamp;
export const float = schemaColumns.float;
export const bytes = schemaColumns.bytes;
export const json = schemaColumns.json;
const enumColumn = schemaColumns.enum;
export { enumColumn as enum };
export const array = schemaColumns.array;

export type { DefinedTable as Table, TableDefinition } from "./schema-dsl/table.js";
export type {
  Uuid,
  Value,
  ValueKind,
  ValueHandle,
  Policy,
  Rel,
  Permissions,
  ColumnHandle,
} from "./permissions/types.js";
export type { DefineSchemaOptions } from "./schema-dsl/define-schema.js";

type TablesOf<TSchema> = TSchema extends { readonly __tables?: infer S } ? NonNullable<S> : never;
type AnySchema = { readonly __tables?: SchemaTables };

/** What `s.defineSchema` returns, for the tables `S`. */
export type Schema<S extends SchemaTables = SchemaTables> = DefinedSchema<S, any>;

/** The row handle of a table: `s.Row<typeof schema, "releases">`. */
export type Row<
  TSchema extends AnySchema,
  TTable extends keyof TablesOf<TSchema> & string,
> = RowOf<TablesOf<TSchema>, TTable>;

/** The row before an update (`updateTo`'s `old`): `s.OldRow<typeof schema, "chats">`. */
export type OldRow<
  TSchema extends AnySchema,
  TTable extends keyof TablesOf<TSchema> & string,
> = OldRowOf<TablesOf<TSchema>, TTable>;

/** A table's condition object: `s.Conditions<typeof schema, "chats">`. */
export type Conditions<
  TSchema extends AnySchema,
  TTable extends keyof TablesOf<TSchema> & string,
> = ConditionsOf<TablesOf<TSchema>, TTable>;
