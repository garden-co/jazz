// Creating a Jazz client for vanilla JavaScript/TypeScript

import type { WasmDatabaseLike } from '@jazz/client';

export interface JazzClientOptions {
  schema: string;
  onReady?: () => void;
}

export async function createJazzClient(options: JazzClientOptions): Promise<WasmDatabaseLike> {
  // Load the WASM module
  const wasm = await import('groove-wasm');
  await wasm.default();

  // Create the database
  const db: WasmDatabaseLike = wasm.Database.new();

  // Execute the schema
  db.execute(options.schema);

  // Call the ready callback if provided
  options.onReady?.();

  return db;
}

// Example usage:
// const db = await createJazzClient({
//   schema: `CREATE TABLE Tasks (title STRING NOT NULL)`,
//   onReady: () => console.log('Jazz is ready!')
// });
