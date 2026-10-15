// Symbols the model-query partition uses but does not own.
import { extern } from "../support/runtime.js";
import type { Reader, Writer } from "../support/postcard.js";

// groove::records::Value — assumed shape `{ kind: "U8" | ... | "Uuid" | "Array" | ..., value }`.
export type Value = any;
// groove::schema::ColumnType — assumed shape `{ kind, inner?, types?, schema?, descriptor? }`.
export type ColumnType = any;
export const columnTypeEq = extern<(a: ColumnType, b: ColumnType) => boolean>();
export const columnTypeDebug = extern<(t: ColumnType) => string>();
export const recordDescriptorEq = extern<(a: any, b: any) => boolean>();
export const encodeValue = extern<(w: Writer, v: Value) => void>();
export const decodeValue = extern<(r: Reader) => Value>();
export const encodeColumnType = extern<(w: Writer, t: ColumnType) => void>();
export const decodeColumnType = extern<(r: Reader) => ColumnType>();

// crate::schema / crate::ids (model-schema partition)
export type JazzSchema = any;
export type RuntimeSchema = { tables: TableSchema[] } & any;
export type TableSchema = {
  name: string;
  columns: { name: string; column_type: ColumnType }[];
  references: Map<string, string>;
};
export type SchemaVersionId = any;
export const APP_COLUMN_PREFIX = extern<string>();
export const newTableSchema =
  extern<
    (name: string, columns: Iterable<{ name: string; column_type: ColumnType }>) => TableSchema
  >();
export const newColumnSchema =
  extern<(name: string, columnType: ColumnType) => { name: string; column_type: ColumnType }>();
export const jazzSchemaRuntime = extern<(schema: JazzSchema) => RuntimeSchema>();
export const runtimeSchemaVersionId = extern<(schema: RuntimeSchema) => SchemaVersionId>();
export const schemaVersionIdBytes = extern<(id: SchemaVersionId) => Uint8Array>();
export const encodeSchemaVersionId = extern<(w: Writer, id: SchemaVersionId) => void>();
export const decodeSchemaVersionId = extern<(r: Reader) => SchemaVersionId>();
/** crate::ids::AuthorSubject::metadata_type */
export const authorSubjectMetadataType = extern<(column: string) => ColumnType | undefined>();
