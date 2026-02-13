/**
 * Resolve and persist a stable sync client ID for a DB config.
 *
 * Client IDs are used by the server to retain per-client sync state across
 * short reconnects. In browsers we persist IDs in localStorage; in other
 * environments we fall back to ephemeral generation unless explicitly set.
 */

import { generateClientId, isValidClientId, resolveClientId } from "./client-id.js";
import type { DbConfig } from "./db.js";

const CLIENT_ID_STORAGE_PREFIX = "jazz:sync-client-id";

function normalizeServerKey(serverUrl?: string): string {
  if (!serverUrl) return "local";
  try {
    return new URL(serverUrl).origin;
  } catch {
    return serverUrl;
  }
}

function storageKey(config: DbConfig): string {
  const env = config.env ?? "dev";
  const userBranch = config.userBranch ?? "main";
  const server = normalizeServerKey(config.serverUrl);
  return `${CLIENT_ID_STORAGE_PREFIX}:${config.appId}:${env}:${userBranch}:${server}`;
}

function getLocalStorage(): Storage | null {
  try {
    if (typeof localStorage === "undefined") return null;
    return localStorage;
  } catch {
    return null;
  }
}

/**
 * Return DbConfig with a resolved sync `clientId`.
 */
export function withResolvedSyncClientId(config: DbConfig): DbConfig {
  if (config.clientId) {
    return { ...config, clientId: resolveClientId(config.clientId) };
  }

  const storage = getLocalStorage();
  if (!storage) {
    return { ...config, clientId: generateClientId() };
  }

  const key = storageKey(config);
  const stored = storage.getItem(key);

  if (stored && isValidClientId(stored)) {
    return { ...config, clientId: stored };
  }

  const clientId = generateClientId();
  storage.setItem(key, clientId);
  return { ...config, clientId };
}
