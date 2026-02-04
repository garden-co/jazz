// Public exports

// DSL for schema definitions
export {
  table,
  col,
  migrate,
  getCollectedSchema,
  getCollectedMigration,
  resetCollectedState,
} from "./dsl.js";
export { schemaToSql, lensToSql } from "./sql-gen.js";
export type {
  Schema,
  Table,
  Column,
  Lens,
  LensOp,
  SqlType,
  LensOpType,
  MigrationOp,
  AddOp,
  DropOp,
  RenameOp,
} from "./schema.js";

// Codegen
export { generateClient, schemaToWasm, generateTypes } from "./codegen/index.js";

// Storage drivers
export * from "./drivers/index.js";

// Runtime client
export * from "./runtime/index.js";
