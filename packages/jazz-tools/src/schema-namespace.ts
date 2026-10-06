import { rel, reverse } from "./relationships.js";
import { schemaColumns, allowExternalProvenanceName } from "./dsl.js";
import { definePermissions } from "./permissions/index.js";
import { defineApp, defineSchema, defineSliceableApp, defineTable } from "./typed-app.js";
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

type RuntimeSchemaNamespace = typeof schemaColumns & {
  rel: typeof rel;
  reverse: typeof reverse;
  table: typeof defineTable;
  defineSchema: typeof defineSchema;
  defineApp: typeof defineApp;
  defineSliceableApp: typeof defineSliceableApp;
  definePermissions: typeof definePermissions;
  allowExternalProvenanceName: typeof allowExternalProvenanceName;
};

/** Schema builders shared by every public binding, including React Native. */
export const schema: RuntimeSchemaNamespace = Object.assign({}, schemaColumns, {
  rel,
  reverse,
  table: defineTable,
  defineSchema,
  defineApp,
  defineSliceableApp,
  definePermissions,
  allowExternalProvenanceName,
} as const);

export namespace schema {
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
}
