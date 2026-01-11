import type { WasmDatabaseLike } from "@jazz/client";

// Mock imports for documentation - these would be real imports in actual usage
declare function initWasm(): Promise<void>;
declare function createWasmDatabase(): WasmDatabaseLike;

//#region create-jazz-client
import init from "groove-wasm";
import wasmUrl from "groove-wasm/groove_wasm_bg.wasm?url";

export async function createJazzClient(): Promise<WasmDatabaseLike> {
  // Initialize the WASM module
  await init(wasmUrl);

  // Create the database instance
  const db = createWasmDatabase();

  return db;
}
//#endregion

//#region init-database
export async function initDatabase(): Promise<WasmDatabaseLike> {
  // Initialize WASM
  await initWasm();

  // Create database instance
  const db = createWasmDatabase();

  // Load schema into the database
  const _schemaSQL = `
    CREATE TABLE Users (
      id STRING NOT NULL,
      name STRING NOT NULL,
      email STRING NOT NULL
    );

    CREATE TABLE Tasks (
      id STRING NOT NULL,
      title STRING NOT NULL,
      description STRING,
      completed BOOL NOT NULL,
      user REFERENCES Users NOT NULL
    );
  `;

  // db.init_schema(schemaSQL);

  return db;
}
//#endregion

//#region schema-import
// Import schema as raw string (Vite)
// import schema from './schema.sql?raw';

// Then initialize with the schema
// db.init_schema(schema);
//#endregion
