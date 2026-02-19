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
  PolicyExpr,
  PolicyOperation,
  PolicyCmpOp,
  PolicyValue,
  OperationPolicy,
  TablePolicies,
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

// Permissions DSL
export * from "./permissions/index.js";

// Local synthetic users and vanilla switcher UI
export {
  createSyntheticUserProfile,
  getActiveSyntheticAuth,
  loadSyntheticUserStore,
  saveSyntheticUserStore,
  setActiveSyntheticProfile,
  syntheticUserStorageKey,
  type ActiveSyntheticAuth,
  type StorageLike,
  type SyntheticUserProfile,
  type SyntheticUserStorageOptions,
  type SyntheticUserStore,
} from "./synthetic-users.js";
export {
  createSyntheticUserSwitcher,
  type SyntheticUserSwitcherHandle,
  type SyntheticUserSwitcherOptions,
} from "./synthetic-user-switcher.js";
