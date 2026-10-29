import { statSync, watch, type FSWatcher } from "node:fs";
import { basename, join } from "node:path";
import { CLIENT_MIGRATIONS_FILE, deploy } from "./catalogue-project.js";
import { toError } from "../runtime/util.js";

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
      const result = await deploy({
        serverUrl: options.serverUrl,
        appId: options.appId,
        adminSecret: options.adminSecret,
        schemaDir: options.schemaDir,
      });
      if (!closed) options.onPush?.(result.schema.hash);
    } catch (error) {
      if (!closed) options.onError?.(toError(error));
    } finally {
      pushing = false;
      if (pendingRetry && !closed) {
        pendingRetry = false;
        void doPush();
      }
    }
  };

  const schedulePush = () => {
    if (closed) return;
    if (debounceTimer) clearTimeout(debounceTimer);
    debounceTimer = setTimeout(() => void doPush(), DEBOUNCE_MS);
  };

  const migrationsDir = join(options.schemaDir, "migrations");
  let migrationWatcher: FSWatcher | undefined;
  let migrationDirectoryIdentity: string | undefined;
  // Watch only the migration sources, not snapshots, lock files, or node_modules.
  const refreshMigrationWatcher = () => {
    const stat = statSync(migrationsDir, { throwIfNoEntry: false });
    const identity = stat?.isDirectory() ? `${stat.dev}:${stat.ino}` : undefined;
    if (identity === migrationDirectoryIdentity) return false;
    migrationWatcher?.close();
    migrationWatcher = undefined;
    migrationDirectoryIdentity = undefined;
    if (identity) {
      migrationWatcher = watch(migrationsDir, (_event, filename) => {
        // The migrations module is deploy's output, not a source.
        if (filename?.endsWith(".ts") && filename !== CLIENT_MIGRATIONS_FILE) schedulePush();
      });
      migrationDirectoryIdentity = identity;
    }
    return true;
  };

  let watcher: FSWatcher | undefined;
  try {
    watcher = watch(options.schemaDir, { recursive: false }, (_event, filename) => {
      if (!filename || closed) return;
      const name = basename(filename);
      if (WATCHED_FILES.has(name)) schedulePush();
      if (name === "migrations") {
        try {
          if (refreshMigrationWatcher()) schedulePush();
        } catch (error) {
          options.onError?.(toError(error));
        }
      }
    });
    refreshMigrationWatcher();
  } catch (error) {
    watcher?.close();
    migrationWatcher?.close();
    throw new Error(
      `Failed to watch ${options.schemaDir}: ${error instanceof Error ? error.message : String(error)}`,
    );
  }

  return {
    close() {
      closed = true;
      pendingRetry = false;
      if (debounceTimer) clearTimeout(debounceTimer);
      watcher?.close();
      migrationWatcher?.close();
    },
  };
}
