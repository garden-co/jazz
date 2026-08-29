import type { RuntimeSourcesConfig } from "./context.js";
import type { DbConfig } from "./db.js";
import { resolveClientInternalSessionSync } from "./client-session.js";
import { getTrustedReservedSession } from "./db-internal-session.js";
import {
  resolveConfiguredUrl,
  resolveRuntimeConfigBrokerWorkerUrl,
  resolveRuntimeConfigWasmUrl,
  versionRuntimeAssetUrl,
} from "./runtime-config.js";

const SHARED_RUNTIME_PROTOCOL_VERSION = "jazz-shared-runtime-v1";
// Coupled to IndexedDbPageStore's durable epoch. Keeping it in the database
// scope prevents an old worker from opening incompatible root metadata.
const BROWSER_STORAGE_FORMAT_VERSION = "idbtree-v1";
const inMemoryWasmAssetIds = new WeakMap<object, string>();

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
  return versionRuntimeAssetUrl(
    resolveConfiguredUrl(bundledUrl, typeof location !== "undefined" ? location.href : undefined),
    runtimeSources,
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
  if (!runtimeSources) return runtimeSources;

  if (
    (runtimeSources.baseUrl || runtimeSources.wasmUrl || runtimeSources.brokerWorkerUrl) &&
    !runtimeSources.wasmVersion
  ) {
    throw new Error(
      "Configured browser runtime assets require runtimeSources.wasmVersion. Use an immutable build version that changes whenever the WASM or worker bytes change.",
    );
  }

  const workerWasmAssetIdentity = inMemoryWasmAssetIdentity(runtimeSources);
  if (workerWasmAssetIdentity) {
    return { ...runtimeSources, workerWasmAssetIdentity };
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
 * asset URL. The complete canonical identity is intentionally kept instead
 * of a short hash: a collision must not let incompatible WASM bytes share a
 * process-global wasm-bindgen module.
 */
export function createBrowserWorkerAssetScope(runtimeSources?: RuntimeSourcesConfig): string {
  const resolvedSources = resolveBrowserWorkerRuntimeSources(runtimeSources);
  return JSON.stringify({
    workerUrl: resolveBrowserWorkerUrl(resolvedSources),
    wasmAsset: workerWasmAssetIdentity(resolvedSources),
  });
}

function inMemoryWasmAssetIdentity(runtimeSources: RuntimeSourcesConfig): string | undefined {
  const [value, kind] = runtimeSources.wasmModule
    ? [runtimeSources.wasmModule, "module"]
    : runtimeSources.wasmSource
      ? [runtimeSources.wasmSource, "source"]
      : [];
  if (!value || !kind) return undefined;
  let identity = inMemoryWasmAssetIds.get(value);
  if (!identity) {
    identity = `${kind}:${crypto.randomUUID()}`;
    inMemoryWasmAssetIds.set(value, identity);
  }
  return identity;
}

function workerWasmAssetIdentity(runtimeSources?: RuntimeSourcesConfig): string {
  if (runtimeSources?.wasmModule) {
    return `module:${runtimeSources.workerWasmAssetIdentity ?? "unscoped"}`;
  }
  if (runtimeSources?.wasmSource) {
    return `source:${runtimeSources.workerWasmAssetIdentity ?? "unscoped"}`;
  }
  return `url:${runtimeSources?.wasmUrl ?? "worker-local"}`;
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
  const session = resolveClientInternalSessionSync({
    appId: config.appId,
    jwtToken: config.jwtToken,
    cookieSession: config.cookieSession,
    trustedReservedSession: getTrustedReservedSession(config),
  });
  if (!session?.user_id || session.authMode === "anonymous") return "anonymous";
  return `${session.authMode}:${JSON.stringify([session.issuer, session.user_id])}`;
}
