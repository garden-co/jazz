// Test setup (not shipped): backend modules a test imports on their own,
// without jazz-tools/backend's entry, get Node's engine. jazz-napi loads on
// first use only, so test files that never touch the backend don't load
// the addon (and don't tear it down at exit).
import { createRequire } from "node:module";
import {
  napiBackendNative,
  provideBackendNative,
  type BackendNative,
} from "../../src/backend/backend-native.js";
import { importNodePeer } from "../../src/backend/node-peers.js";

let engine: BackendNative | undefined;
const load = (): BackendNative =>
  (engine ??= napiBackendNative(createRequire(import.meta.url)("jazz-napi"), importNodePeer));

provideBackendNative(
  new Proxy({} as BackendNative, {
    get: (_target, key) => load()[key as keyof BackendNative],
  }),
);
