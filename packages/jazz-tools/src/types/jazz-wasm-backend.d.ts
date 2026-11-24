// `jazz-wasm/backend`: jazz-wasm's backend build, initialised on import
// (crates/jazz-wasm/backend/index.js). The browser build's API, plus what
// only a backend's `createJazz` uses (crates/jazz-wasm/src/backend.rs),
// typed loosely here: `BackendNative` gives them their shapes.
declare module "jazz-wasm/backend" {
  export * from "jazz-wasm";
  export const WasmThinRouter: unknown;
  export function admitPrelude(prelude: Uint8Array, facts: string): string;
  export function pinPreludeBundle(prelude: Uint8Array, bundle?: string): Uint8Array | undefined;
  export function loadBackendApp(schema: string, permissions: string): unknown;
}
