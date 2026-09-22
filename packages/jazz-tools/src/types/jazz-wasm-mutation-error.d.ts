import "jazz-wasm";
import type { MutationErrorEvent } from "../runtime/client.js";

/**
 * The wasm-bindgen package is generated at build time. Keep the runtime's
 * rejection callback contract available to TypeScript while a workspace still
 * contains artifacts generated before the binding was added.
 */
declare module "jazz-wasm" {
  interface WasmDb {
    onMutationError(callback: (event: MutationErrorEvent) => void): void;
  }
}
