import type { RuntimeSourcesConfig } from "./context.js";
import type { DbConfig } from "./db.js";
import { resolveClientSessionSync } from "./client-session.js";
import { resolveConfiguredUrl, resolveRuntimeConfigBrokerWorkerUrl } from "./runtime-config.js";

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
  });
  if (!session?.user_id || session.authMode === "anonymous") return "anonymous";
  return `${session.authMode}:${session.user_id}`;
}
