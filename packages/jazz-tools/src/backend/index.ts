// Node's engine: jazz-napi, through a static import bundlers and file
// tracers follow (Next.js, Vercel), so a deployed backend ships the addon.
// A Cloudflare Worker loads ./workerd.js instead, on jazz-wasm.
import * as jazzNapi from "jazz-napi";
import { napiBackendNative, provideBackendNative } from "./backend-native.js";
import { importNodePeer } from "./node-peers.js";

provideBackendNative(napiBackendNative(jazzNapi, importNodePeer));

export * from "./api.js";
