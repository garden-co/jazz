import type { RuntimeSourcesConfig } from "./context.js";
import type { DbConfig } from "./db.js";
import { resolveClientInternalSessionSync } from "./client-session.js";
import { getTrustedReservedSession } from "./db-internal-session.js";
import { canonicalAuthorSubject } from "./author-id.js";
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

/**
 * Exact, non-secret authentication scope carried by browser worker and
 * durable-owner metadata.
 *
 * This deliberately names the authenticated principal rather than hashing it:
 * the identifier is already the canonical public `session.user` encoding
 * (`[issuer, subject]`), and a collision here could give two accounts the same
 * worker or physical store. Tokens, secrets, claims, expiry and other
 * credentials are never included. Anonymous callers intentionally share their
 * app/environment scope; a browser persistent worker never admits a backend,
 * but `system` keeps this representation total for defensive callers.
 */
type BrowserAuthScope =
  | { kind: "anonymous" }
  | { kind: "system" }
  | {
      kind: "principal";
      authMode: "external" | "local-first";
      user: string;
    };

function browserAuthScope(config: DbConfig): BrowserAuthScope {
  if (config.adminSecret) return { kind: "system" };

  const session = resolveClientInternalSessionSync({
    appId: config.appId,
    jwtToken: config.jwtToken,
    cookieSession: config.cookieSession,
    trustedReservedSession: getTrustedReservedSession(config),
  });
  if (!session || session.authMode === "anonymous") return { kind: "anonymous" };

  return {
    kind: "principal",
    authMode: session.authMode,
    // Reuse the public/session and row-authorship identity codec. JSON array
    // encoding preserves exact issuer + subject boundaries and spelling.
    user: canonicalAuthorSubject(session.issuer, session.user_id),
  };
}

/** Stable, non-secret exact namespace for one browser authentication scope. */
export function createBrowserAuthSessionKey(config: DbConfig): string {
  // Authentication scopes are also app/environment-scoped. Two apps may use
  // the same external provider subject without sharing a relay lifecycle.
  return JSON.stringify({
    version: 1,
    appId: config.appId,
    env: config.env ?? "dev",
    auth: browserAuthScope(config),
  });
}

/**
 * Stable, non-secret owner identity for a physical browser persistence root.
 * This is intentionally separate from the IDB name (physical location) and
 * from the foreground replica/node identity. An explicitly supplied name is
 * therefore still safe: it is permanently bound to one app/environment/auth
 * scope until the caller explicitly destroys that database.
 */
export function createBrowserStorageOwner(config: DbConfig): string {
  // This is the exact durable value stored under
  // INDEXEDDB_BROWSER_RUNTIME_OWNER_KEY. Keep it canonical JSON: changing its
  // shape is a deliberate browser-owner-marker schema migration, not a hash
  // implementation detail. The marker contains only app/environment and the
  // auth scope above, never a credential or arbitrary claims.
  return JSON.stringify({
    version: 1,
    appId: config.appId,
    env: config.env ?? "dev",
    auth: browserAuthScope(config),
  });
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
