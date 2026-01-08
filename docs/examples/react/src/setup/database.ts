import type { WasmDatabaseLike } from "@jazz/client";

// Mock types for documentation
declare function wasmInit(): Promise<void>;
declare function createWasmDb(): WasmDatabaseLike & {
  init_schema(sql: string): void;
  list_tables(): string[];
};

//#region init-wasm-database
import init, { WasmDatabase } from "groove-wasm";

export async function initDatabase(): Promise<WasmDatabaseLike> {
  // Initialize the WASM module
  await init();

  // Create an in-memory database
  const db = new WasmDatabase();

  // Initialize your schema
  const schemaSQL = `
    CREATE TABLE Users (
      id STRING NOT NULL,
      name STRING NOT NULL,
      email STRING NOT NULL
    );
  `;
  db.init_schema(schemaSQL);

  return db;
}
//#endregion

//#region schema-raw-import
// Import schema as raw string using Vite
import schema from "./schema.sql?raw";

// Then initialize with the imported schema
function initWithSchema(db: ReturnType<typeof createWasmDb>) {
  db.init_schema(schema);
}
//#endregion

//#region db-init-schema
function initSchema(db: ReturnType<typeof createWasmDb>, schemaSQL: string) {
  db.init_schema(schemaSQL);
}
//#endregion

//#region db-list-tables
function listTables(db: ReturnType<typeof createWasmDb>) {
  const tables = db.list_tables();
  console.log("Tables:", tables); // ['Users', 'Projects', 'Tasks', ...]
}
//#endregion

export { initWithSchema, initSchema, listTables };
