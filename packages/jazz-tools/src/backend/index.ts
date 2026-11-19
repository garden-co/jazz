// `jazz-tools/backend` under Node, on jazz-napi (see node-native.ts). A
// Cloudflare Worker loads ./workerd.js instead, on jazz-wasm.
import {
  createJazz as createJazzOnEngine,
  createJazzClient as createJazzClientOnEngine,
  type BackendClientSession,
  type CreateJazzOptions,
  type Jazz,
  type JazzClientConfig,
} from "./api.js";
import { installNodeEngine } from "./node-native.js";

export * from "./api.js";

// Defined here, not re-exported, and calling `installNodeEngine` by name:
// bundlers that resolve a barrel's re-exports themselves (Turbopack) never
// evaluate this module otherwise, and a deployed function would have no engine.

/** {@inheritDoc createJazzOnEngine} */
export function createJazz(options: CreateJazzOptions): Jazz {
  installNodeEngine();
  return createJazzOnEngine(options);
}

/** {@inheritDoc createJazzClientOnEngine} */
export function createJazzClient(input: JazzClientConfig): Promise<BackendClientSession> {
  installNodeEngine();
  return createJazzClientOnEngine(input);
}
