/// <reference types="vite/client" />

// Declare Vite-specific import patterns
declare module "*.sql?raw" {
  const content: string;
  export default content;
}

declare module "*.wasm?url" {
  const url: string;
  export default url;
}

// Mock groove-wasm module for type checking
declare module "groove-wasm" {
  export default function init(url?: string | URL): Promise<void>;
  export class WasmDatabase {
    constructor();
    init_schema(sql: string): void;
    execute(sql: string): unknown;
    subscribeDelta(
      sql: string,
      callback: (deltas: Uint8Array[]) => void,
    ): { diagram(): string; unsubscribe(): void; free(): void };
    updateRow(
      table: string,
      row_id: string,
      column: string,
      value: string,
    ): boolean;
    updateRowI64(
      table: string,
      row_id: string,
      column: string,
      value: bigint,
    ): boolean;
    list_tables(): string[];
  }
}

declare module "groove-wasm/groove_wasm_bg.wasm?url" {
  const url: string;
  export default url;
}
