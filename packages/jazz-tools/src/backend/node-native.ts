/**
 * Installs jazz-napi as the backend's engine, through a static import that
 * bundlers and file tracers (Next.js, Vercel) follow, so a deployed backend
 * ships the addon. Every Node entry that reaches backend code imports this
 * module: `jazz-tools/backend`, `jazz-tools/testing` and the Effect backend.
 * A Cloudflare Worker loads `workerd.ts` instead, on jazz-wasm.
 */
import * as jazzNapi from "jazz-napi";
import { napiBackendNative, provideBackendNative } from "./backend-native.js";
import { importNodePeer } from "./node-peers.js";

provideBackendNative(napiBackendNative(jazzNapi, importNodePeer));
