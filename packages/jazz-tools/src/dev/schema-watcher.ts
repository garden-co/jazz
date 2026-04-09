import { watch, type FSWatcher } from "node:fs";
import { basename } from "node:path";
import { pushSchemaCatalogue } from "./dev-server.js";

export interface SchemaWatcherOptions {
  schemaDir: string;
  serverUrl: string;
  appId: string;
  adminSecret: string;
  onPush?: (hash: string) => void;
  onError?: (error: Error) => void;
}

const WATCHED_FILES = new Set(["schema.ts", "permissions.ts"]);
const DEBOUNCE_MS = 200;

export function watchSchema(options: SchemaWatcherOptions): { close: () => void } {
  let debounceTimer: ReturnType<typeof setTimeout> | null = null;
  let pushing = false;
  let pendingRetry = false;
  let closed = false;

  const doPush = async () => {
    if (closed || pushing) {
      if (pushing) pendingRetry = true;
      return;
    }
    pushing = true;
    try {
      const { hash } = await pushSchemaCatalogue({
        serverUrl: options.serverUrl,
        appId: options.appId,
        adminSecret: options.adminSecret,
        schemaDir: options.schemaDir,
      });
      if (!closed) options.onPush?.(hash);
    } catch (error) {
      if (!closed) options.onError?.(error instanceof Error ? error : new Error(String(error)));
    } finally {
      pushing = false;
      if (pendingRetry && !closed) {
        pendingRetry = false;
        void doPush();
      }
    }
  };

  let watcher: FSWatcher;
  try {
    watcher = watch(options.schemaDir, { recursive: false }, (_event, filename) => {
      if (!filename || !WATCHED_FILES.has(basename(filename))) return;
      if (debounceTimer) clearTimeout(debounceTimer);
      debounceTimer = setTimeout(() => void doPush(), DEBOUNCE_MS);
    });
  } catch (error) {
    throw new Error(
      `Failed to watch ${options.schemaDir}: ${error instanceof Error ? error.message : String(error)}`,
    );
  }

  return {
    close() {
      closed = true;
      pendingRetry = false;
      if (debounceTimer) clearTimeout(debounceTimer);
      watcher.close();
    },
  };
}
