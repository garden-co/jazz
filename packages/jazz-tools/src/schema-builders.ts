/**
 * The members of the `schema` namespace (`schema-namespace.ts`).
 *
 * Each builder is its own export so bundlers can drop the ones an app never
 * names. An object literal holding them all kept every builder, including
 * `definePermissions` and the whole permissions DSL, in every client bundle.
 */
import { schemaColumns } from "./dsl.js";
import type {
  App as TypedApp,
  InsertOf as TypedInsertOf,
  LargeValueUpdateOf as TypedLargeValueUpdateOf,
  StreamingInsertOf as TypedStreamingInsertOf,
  StreamingUpdateOf as TypedStreamingUpdateOf,
  StreamingUpsertOf as TypedStreamingUpsertOf,
  RowOf as TypedRowOf,
  Schema as TypedSchema,
  SchemaDefinition as TypedSchemaDefinition,
  SliceableApp as TypedSliceableApp,
  TableDefinition as TypedTableDefinition,
  TableMetaOf as TypedTableMetaOf,
  WhereOf as TypedWhereOf,
} from "./typed-app.js";

export { rel, reverse } from "./relationships.js";
export { allowExternalProvenanceName } from "./dsl.js";
export { definePermissions } from "./permissions/index.js";
export {
  defineApp,
  defineSchema,
  defineSliceableApp,
  defineTable as table,
  defineComputed as computed,
} from "./typed-app.js";

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

export type TableDefinition = TypedTableDefinition;
export type SchemaDefinition = TypedSchemaDefinition;
export type Schema<TSchema extends TypedSchemaDefinition = TypedSchemaDefinition> =
  TypedSchema<TSchema>;
export type App<TSchema extends TypedSchema<any> | TypedSchemaDefinition> = TypedApp<TSchema>;
export type SliceableApp<TSchema extends TypedSchema<any> | TypedSchemaDefinition> =
  TypedSliceableApp<TSchema>;
export type RowOf<TTable> = TypedRowOf<TTable>;
export type InsertOf<TTable> = TypedInsertOf<TTable>;
export type LargeValueUpdateOf<TTable> = TypedLargeValueUpdateOf<TTable>;
export type StreamingInsertOf<TTable> = TypedStreamingInsertOf<TTable>;
export type StreamingUpdateOf<TTable> = TypedStreamingUpdateOf<TTable>;
export type StreamingUpsertOf<TTable> = TypedStreamingUpsertOf<TTable>;
export type TableMetaOf<TTable> = TypedTableMetaOf<TTable>;
export type WhereOf<TQuery> = TypedWhereOf<TQuery>;
