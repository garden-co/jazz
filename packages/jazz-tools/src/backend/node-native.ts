/**
 * Installs jazz-napi as the backend's engine, through a static import that
 * bundlers and file tracers (Next.js, Vercel) follow, so a deployed backend
 * ships the addon. Node's entries call {@link installNodeEngine} by direct
 * reference before any backend code runs (`jazz-tools/backend`'s
 * `createJazz`/`createJazzClient`, `jazz-tools/testing`, the Effect
 * backend): a bundler can drop a side-effect import (Turbopack skips a
 * barrel module whose re-exports it resolves itself), never a used binding.
 * A Cloudflare Worker loads `workerd.ts` instead, on jazz-wasm.
 */
import * as jazzNapi from "jazz-napi";
import { napiBackendNative, provideBackendNative } from "./backend-native.js";
import { importNodePeer } from "./node-peers.js";

/** Install jazz-napi as the backend's engine (once; later calls do nothing). */
export function installNodeEngine(): void {
  provideBackendNative(napiBackendNative(jazzNapi, importNodePeer));
}

installNodeEngine();
