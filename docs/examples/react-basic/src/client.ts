import { useState, useEffect } from 'react';
import type { WasmDatabaseLike } from '@jazz/client';

// Initialize the Jazz client with WASM
export function useJazzClient(): WasmDatabaseLike | null {
  const [client, setClient] = useState<WasmDatabaseLike | null>(null);

  useEffect(() => {
    async function init() {
      // Load the WASM module
      const wasm = await import('groove-wasm');
      await wasm.default();

      // Create the database with schema
      const db = wasm.Database.new();
      db.execute(`
        CREATE TABLE Tasks (
          title STRING NOT NULL,
          description STRING,
          completed BOOLEAN NOT NULL,
          priority STRING NOT NULL,
          createdAt I64 NOT NULL
        )
      `);

      setClient(db);
    }

    init();
  }, []);

  return client;
}
