/**
 * The engine a backend runs on: jazz-napi under Node, jazz-wasm in a
 * Cloudflare Worker (workerd), where native addons can't load. Backend
 * modules reach it through {@link backendNative} instead of importing
 * jazz-napi, so a Worker bundle never pulls the addon in.
 *
 * Node's entries provide jazz-napi through `node-native.ts`, a static import
 * bundlers and file tracers follow; the workerd entry provides jazz-wasm.
 * Either runs before any backend code does.
 */
import type * as Napi from "jazz-napi";

export interface BackendNative {
  /** Where it runs: `node` (jazz-napi) or `workerd` (jazz-wasm). */
  readonly host: "node" | "workerd";
  /** The engine's database (NapiDb, or WasmDb with the same backend methods). */
  readonly Db: typeof Napi.NapiDb;
  /** Routes thin clients' transactions between their backend and its core. */
  readonly ThinRouter: typeof Napi.NapiThinRouter;
  /** An in-process core (`sqlite()`, `memory()`): Node only. */
  readonly JazzCore?: typeof Napi.JazzCore;
  readonly captureTextAnchor: typeof Napi.captureTextAnchor;
  readonly captureTextRange: typeof Napi.captureTextRange;
  readonly resolveTextAnchor: typeof Napi.resolveTextAnchor;
  readonly resolveTextRange: typeof Napi.resolveTextRange;
  /** What `provideCodegenNapi` takes: the app-file loader and code generator. */
  readonly codegen: unknown;
  /** Load an optional peer package (Node only; a Worker has none). */
  readonly importPeer: (peer: NodePeer) => Promise<unknown>;
}

/** The optional peer packages a Node backend may load. */
export type NodePeer = "ws" | "@vercel/functions" | "@vercel/functions/oidc" | "@vercel/sandbox";

let provided: BackendNative | undefined;

/** Install the engine. The first one installed stays. */
export function provideBackendNative(native: BackendNative): void {
  provided ??= native;
}

/** jazz-napi as a {@link BackendNative}. */
export function napiBackendNative(
  napi: typeof Napi,
  importPeer: (peer: NodePeer) => Promise<unknown> = (peer) =>
    // Not a specifier bundlers follow: Node's entry hands over literal ones.
    import(/* @vite-ignore */ /* webpackIgnore: true */ peer),
): BackendNative {
  return {
    host: "node",
    Db: napi.NapiDb,
    ThinRouter: napi.NapiThinRouter,
    JazzCore: napi.JazzCore,
    captureTextAnchor: napi.captureTextAnchor,
    captureTextRange: napi.captureTextRange,
    resolveTextAnchor: napi.resolveTextAnchor,
    resolveTextRange: napi.resolveTextRange,
    codegen: napi,
    importPeer,
  };
}

/** The installed engine. */
export function backendNative(): BackendNative {
  if (provided) return provided;
  throw new Error(
    "jazz-tools has no backend engine installed: import createJazz from jazz-tools/backend (Node installs jazz-napi; a Cloudflare Worker, under the workerd condition, jazz-wasm).",
  );
}

/** The in-process core, refused where it can't run. */
export function inProcessCore(store: string): typeof Napi.JazzCore {
  const core = backendNative().JazzCore;
  if (!core) {
    throw new Error(
      `${store}() runs its core in this process, which a Cloudflare Worker can't: use durableObjects() there.`,
    );
  }
  return core;
}
