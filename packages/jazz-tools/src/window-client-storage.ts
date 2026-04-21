import type { DbConfig } from "./runtime/db.js";
import { resolveClientSessionSync } from "./runtime/client-session.js";

type WindowJazzStorageDb = {
  getConfig(): DbConfig;
  deleteClientStorage(): Promise<void>;
};

type LiveStorageContext = {
  db: WindowJazzStorageDb;
  namespace: string;
};

export interface WindowJazzClientApi {
  clearStorage(namespace?: string): Promise<void>;
  listLiveStorageNamespaces(): string[];
}

declare global {
  interface Window {
    __jazz?: WindowJazzClientApi;
    __jazzWindowStorageContexts__?: Set<LiveStorageContext>;
  }
}

function resolveStorageNamespace(config: DbConfig): string | null {
  const driver = config.driver ?? { type: "persistent" };
  if (driver.type !== "persistent") {
    return null;
  }

  const explicitDbName = driver.dbName?.trim() || config.dbName?.trim();
  if (explicitDbName) {
    return explicitDbName;
  }

  const sessionUserId = resolveClientSessionSync({
    appId: config.appId,
    jwtToken: config.jwtToken,
  })?.user_id;

  if (!sessionUserId) {
    return config.appId;
  }

  return `${config.appId}::${encodeURIComponent(sessionUserId)}`;
}

function getLiveStorageContexts(currentWindow: Window): Set<LiveStorageContext> {
  if (!currentWindow.__jazzWindowStorageContexts__) {
    currentWindow.__jazzWindowStorageContexts__ = new Set();
  }

  return currentWindow.__jazzWindowStorageContexts__;
}

function listLiveStorageNamespaces(currentWindow: Window): string[] {
  return [
    ...new Set([...getLiveStorageContexts(currentWindow)].map((entry) => entry.namespace)),
  ].sort();
}

function formatAvailableNamespaces(namespaces: readonly string[]): string {
  if (namespaces.length === 0) {
    return "";
  }

  return ` Available namespaces: ${namespaces.join(", ")}.`;
}

function resolveStorageTarget(currentWindow: Window, namespace?: string): WindowJazzStorageDb {
  const liveContexts = [...getLiveStorageContexts(currentWindow)];
  const namespaces = listLiveStorageNamespaces(currentWindow);

  if (liveContexts.length === 0) {
    throw new Error("No live Jazz storage contexts are available on this page.");
  }

  const trimmedNamespace = namespace?.trim();
  if (!trimmedNamespace) {
    if (namespaces.length === 1) {
      const [onlyNamespace] = namespaces;
      return liveContexts.find((entry) => entry.namespace === onlyNamespace)!.db;
    }

    throw new Error(
      `Multiple live Jazz storage contexts are available. Call window.__jazz.clearStorage("<namespace>").${formatAvailableNamespaces(
        namespaces,
      )}`,
    );
  }

  const match = liveContexts.find((entry) => entry.namespace === trimmedNamespace);
  if (!match) {
    throw new Error(
      `No live Jazz storage context matches "${trimmedNamespace}".${formatAvailableNamespaces(
        namespaces,
      )}`,
    );
  }

  return match.db;
}

function ensureWindowJazzApi(currentWindow: Window): WindowJazzClientApi {
  const existing = currentWindow.__jazz;
  const apiHost =
    existing && typeof existing === "object"
      ? (existing as unknown as Record<string, unknown>)
      : {};

  const api = Object.assign(apiHost, {
    async clearStorage(namespace?: string): Promise<void> {
      const target = resolveStorageTarget(currentWindow, namespace);
      await target.deleteClientStorage();
    },
    listLiveStorageNamespaces(): string[] {
      return listLiveStorageNamespaces(currentWindow);
    },
  }) as WindowJazzClientApi;

  currentWindow.__jazz = api;
  return api;
}

export function registerWindowJazzStorageClient(db: WindowJazzStorageDb): () => void {
  if (typeof window === "undefined") {
    return () => {};
  }

  const namespace = resolveStorageNamespace(db.getConfig());
  if (!namespace) {
    return () => {};
  }

  const currentWindow = window;
  ensureWindowJazzApi(currentWindow);

  const contexts = getLiveStorageContexts(currentWindow);
  const context: LiveStorageContext = { db, namespace };
  contexts.add(context);

  return () => {
    currentWindow.__jazzWindowStorageContexts__?.delete(context);
  };
}
