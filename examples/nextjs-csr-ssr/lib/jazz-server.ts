import "server-only";

import { app as schemaApp } from "../schema";
import permissions from "../permissions";
import type { BackendContextConfig, Db, JazzContext } from "jazz-tools/backend";

// This is a workaround to resolve correctly NAPI modules in the monorepo
// Real-world apps should just `import { createJazzContext } from "jazz-tools/backend"`
import { createRequire as createRequireFromModule } from "node:module";
const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const nodeRequire = createRequire(import.meta.url);

type BackendState = { context: JazzContext; db: Db };
type BackendModule = {
  createJazzContext: (config: BackendContextConfig) => JazzContext;
};
type GlobalBackendState = typeof globalThis & {
  __jazzNextCsrSsrBackend?: BackendState;
};

const globalState = globalThis as GlobalBackendState;

function readBackendConfig(): {
  appId: string;
  serverUrl: string;
  backendSecret: string;
} {
  const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID;
  const serverUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL;
  const backendSecret = process.env.BACKEND_SECRET;
  const missingKeys = [
    appId ? undefined : "NEXT_PUBLIC_JAZZ_APP_ID",
    serverUrl ? undefined : "NEXT_PUBLIC_JAZZ_SERVER_URL",
    backendSecret ? undefined : "BACKEND_SECRET",
  ].filter((key): key is string => key !== undefined);
  if (missingKeys.length > 0) {
    throw new Error(`Missing server backend configuration: ${missingKeys.join(", ")}`);
  }
  return { appId: appId!, serverUrl: serverUrl!, backendSecret: backendSecret! };
}

export function getBackendDb(): Db {
  const existing = globalState.__jazzNextCsrSsrBackend;
  if (existing) return existing.db;

  const config = readBackendConfig();
  const { createJazzContext } = nodeRequire("jazz-tools/backend") as BackendModule;
  const context = createJazzContext({
    appId: config.appId,
    app: schemaApp,
    permissions,
    driver: { type: "memory" },
    serverUrl: config.serverUrl,
    backendSecret: config.backendSecret,
  });
  const db = context.asBackend();
  globalState.__jazzNextCsrSsrBackend = { context, db };
  return db;
}
