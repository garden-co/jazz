import type { RuntimeSourcesConfig } from "./context.js";
import type { DbConfig } from "./db.js";
import { resolveClientSessionSync } from "./client-session.js";
import {
  resolveConfiguredUrl,
  resolveRuntimeConfigBrokerWorkerUrl,
  resolveRuntimeConfigWasmUrl,
} from "./runtime-config.js";

const SHARED_RUNTIME_PROTOCOL_VERSION = "jazz-shared-runtime-v1";
const BROWSER_STORAGE_FORMAT_VERSION = "idbtree-v1";

/** Resolve the exact script URL that identifies the origin-wide SharedWorker. */
export function resolveBrowserWorkerUrl(runtimeSources?: RuntimeSourcesConfig): string {
  if (runtimeSources?.brokerWorkerUrl || runtimeSources?.baseUrl) {
    return resolveRuntimeConfigBrokerWorkerUrl(
      import.meta.url,
      typeof location !== "undefined" ? location.href : undefined,
      runtimeSources,
    );
  }
  // Keep this literal statically analyzable so bundlers emit the worker asset.
  const bundledUrl = new URL("../worker/jazz-broker-worker.js", import.meta.url).href;
  return resolveConfiguredUrl(
    bundledUrl,
    typeof location !== "undefined" ? location.href : undefined,
  );
}

/**
 * Resolve page-owned asset configuration before it crosses into a SharedWorker.
 *
 * A relative `wasmUrl` or `baseUrl` belongs to the page which selected it, not
 * to the worker script.  Those can legitimately have different origins during
 * Vite development, a rolling deployment, or a test server restart.  Sending
 * the relative form lets a long-lived worker reinterpret it relative to its
 * own (possibly stale) script URL and fetch an HTML fallback as WASM.
 */
export function resolveBrowserWorkerRuntimeSources(
  runtimeSources?: RuntimeSourcesConfig,
): RuntimeSourcesConfig | undefined {
  if (!runtimeSources || runtimeSources.wasmModule || runtimeSources.wasmSource) {
    return runtimeSources;
  }
  const wasmUrl = resolveRuntimeConfigWasmUrl(
    import.meta.url,
    typeof location !== "undefined" ? location.href : undefined,
    runtimeSources,
  );
  if (!wasmUrl) return runtimeSources;
  return { ...runtimeSources, wasmUrl };
}

/**
 * Name SharedWorker realms by the code/asset pair they can safely host.
 *
 * A worker module owns one wasm-bindgen initialization for its entire process
 * lifetime.  Keeping this scope out of the database name allows an old Vite
 * origin or an earlier deployment to keep serving a new tab with its stale
 * asset URL.  The short deterministic hash keeps the browser-visible worker
 * name bounded while retaining origin and cache-buster identity.
 */
export function createBrowserWorkerAssetScope(runtimeSources?: RuntimeSourcesConfig): string {
  const resolvedSources = resolveBrowserWorkerRuntimeSources(runtimeSources);
  const identity = JSON.stringify({
    workerUrl: resolveBrowserWorkerUrl(resolvedSources),
    wasmUrl: resolvedSources?.wasmUrl ?? "worker-local",
    wasmSource: resolvedSources?.wasmSource ? "provided" : undefined,
    wasmModule: resolvedSources?.wasmModule ? "provided" : undefined,
  });
  let hash = 0x811c9dc5;
  for (let index = 0; index < identity.length; index += 1) {
    hash ^= identity.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193);
  }
  return (hash >>> 0).toString(16).padStart(8, "0");
}

export function createBrowserWorkerFingerprint(
  config: DbConfig,
  dbName: string,
  schemaHash: string,
): string {
  return JSON.stringify({
    protocolVersion: SHARED_RUNTIME_PROTOCOL_VERSION,
    storageFormatVersion: BROWSER_STORAGE_FORMAT_VERSION,
    appId: config.appId,
    dbName,
    env: config.env ?? "dev",
    schemaHash,
    authClass: resolveAuthClass(config),
    workerUrl: resolveBrowserWorkerUrl(config.runtimeSources),
    workerAssetScope: createBrowserWorkerAssetScope(config.runtimeSources),
  });
}

/** Stable, non-secret namespace for one browser authentication session. */
export function createBrowserAuthSessionKey(config: DbConfig): string {
  // Authentication sessions are scoped to a Jazz app/environment. Two apps
  // may both be anonymous (or use the same subject string) without sharing a
  // relay, storage lifecycle, WebSocket, or evaluator. Tabs in the same app
  // and auth session still resolve to one SharedWorker.
  const value = `${config.appId}:${config.env ?? "dev"}:${resolveAuthClass(config)}`;
  let hash = 0x811c9dc5;
  let output = "";
  for (let round = 0; round < 4; round += 1) {
    for (let index = 0; index < value.length; index += 1) {
      hash ^= value.charCodeAt(index) + round;
      hash = Math.imul(hash, 0x01000193);
    }
    output += (hash >>> 0).toString(16).padStart(8, "0");
  }
  return output;
}

function resolveAuthClass(config: DbConfig): string {
  if (config.adminSecret) return "admin";
  const session = resolveClientSessionSync({
    appId: config.appId,
    jwtToken: config.jwtToken,
    cookieSession: config.cookieSession,
    trustedReservedSession: config.trustedReservedSession,
  });
  if (!session?.user_id || session.authMode === "anonymous") return "anonymous";
  return `${session.authMode}:${JSON.stringify([session.issuer, session.user_id])}`;
}
