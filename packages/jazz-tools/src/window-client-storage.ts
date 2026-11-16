import { resolveDefaultPersistentDbName, type DbConfig } from "./runtime/db.js";
import {
  ensureWindowJazz,
  getLiveStorageContexts,
  type LiveStorageContext,
  type WindowJazzStorageDb,
} from "./runtime/page-handle.js";

export type { WindowJazzClientApi } from "./runtime/page-handle.js";

function resolveStorageNamespace(config: DbConfig): string | null {
  const driver = config.driver ?? { type: "persistent" };
  if (driver.type !== "persistent") {
    return null;
  }

  return resolveDefaultPersistentDbName(config);
}

export function registerWindowJazzStorageClient(
  db: WindowJazzStorageDb & { getConfig(): DbConfig },
): () => void {
  if (typeof window === "undefined") {
    return () => {};
  }

  const namespace = resolveStorageNamespace(db.getConfig());
  if (!namespace) {
    return () => {};
  }

  const currentWindow = window;
  ensureWindowJazz(currentWindow);

  const contexts = getLiveStorageContexts(currentWindow);
  const context: LiveStorageContext = { db, namespace };
  contexts.add(context);

  return () => {
    currentWindow.__jazzWindowStorageContexts__?.delete(context);
  };
}
