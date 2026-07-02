import type { StorageDriver } from "../../drivers/types.js";
import type { DbConfig } from "../db.js";
import { resolveClientSessionSync } from "../client-session.js";
import {
  createBrowserBrokerFingerprint,
  createRandomId,
  createRuntimeSourceIdentity,
  type BrowserBrokerVisibility,
} from "../browser-broker-protocol.js";
import { resolveBrokerWorkerUrl } from "../browser-broker-client.js";

export const BROKER_STORAGE_DELETE_MAX_RETRIES = 8;
const BROKER_STORAGE_DELETE_RETRY_BASE_MS = 50;
const BROKER_STORAGE_DELETE_RETRY_MAX_MS = 500;

export function resolveStorageDriver(driver?: StorageDriver): StorageDriver {
  return driver ?? { type: "persistent" };
}

function trimOptionalString(value?: string | null): string | null {
  if (typeof value !== "string") {
    return null;
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

export function sleepMs(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function isBrowser(): boolean {
  return typeof Worker !== "undefined" && typeof window !== "undefined";
}

export function isBrokerStorageLockedError(error: unknown): boolean {
  const name = (error as { name?: string } | undefined)?.name;
  return name === "NoModificationAllowedError" || name === "InvalidStateError";
}

export function brokerStorageDeleteRetryDelayMs(retry: number): number {
  return Math.min(
    BROKER_STORAGE_DELETE_RETRY_BASE_MS * 2 ** retry,
    BROKER_STORAGE_DELETE_RETRY_MAX_MS,
  );
}

export function createBrowserTabId(): string {
  return createRandomId();
}

/** @internal Derive the default browser persistence namespace for this Db config. */
export function resolveDefaultPersistentDbName(config: DbConfig): string {
  const driver = resolveStorageDriver(config.driver);
  const explicitDbName = trimOptionalString(
    (driver.type === "persistent" ? driver.dbName : undefined) ?? config.dbName,
  );
  if (explicitDbName) {
    return explicitDbName;
  }

  const session = resolveClientSessionSync({
    appId: config.appId,
    jwtToken: config.jwtToken,
  });

  if (!session?.user_id || session.authMode === "anonymous") {
    return config.appId;
  }

  return `${config.appId}::${encodeURIComponent(session.user_id)}`;
}

export function currentBrokerVisibility(): BrowserBrokerVisibility {
  if (typeof document === "undefined") {
    return "visible";
  }
  return document.visibilityState === "visible" ? "visible" : "hidden";
}

function resolveBrokerAuthClass(config: DbConfig): string {
  if (config.adminSecret) {
    return "admin";
  }

  const session = resolveClientSessionSync({
    appId: config.appId,
    jwtToken: config.jwtToken,
    cookieSession: config.cookieSession,
  });
  if (!session?.user_id || session.authMode === "anonymous") {
    return "anonymous";
  }
  return `${session.authMode}:${session.user_id}`;
}

export function createBrokerFingerprint(config: DbConfig, primaryDbName: string): string {
  const driver = resolveStorageDriver(config.driver);
  return createBrowserBrokerFingerprint({
    appId: config.appId,
    dbName: primaryDbName,
    persistentDriverNamespace:
      driver.type === "persistent" ? (driver.dbName ?? primaryDbName) : primaryDbName,
    env: config.env ?? "dev",
    userBranch: config.userBranch ?? "main",
    serverUrl: config.serverUrl ?? null,
    schemaHash: null,
    authClass: resolveBrokerAuthClass(config),
    // Key *only* on the resolved broker worker URL, not the rest of the raw
    // config.runtimeSources shape: baseUrl/workerUrl/wasmUrl/wasmModule/
    // wasmSource don't affect which SharedWorker gets constructed (baseUrl's
    // effect is already folded into the resolved URL), so including them here
    // would fingerprint two clients differently even though they load the same
    // broker. This is also why the inspector overlay (a separate bundle that
    // forwards only `brokerWorkerUrl`, none of the other runtimeSources fields)
    // can join the host's broker: both resolve to "default" unless the host set
    // one explicitly.
    runtimeSourceIdentity: createRuntimeSourceIdentity({
      brokerWorkerUrl: resolveBrokerWorkerUrl(config.runtimeSources),
    }),
  });
}
