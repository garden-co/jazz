import "server-only";

import { app as schemaApp } from "../schema";
import permissions from "../permissions";
import type { JazzSessionConfig, Db } from "jazz-tools/backend";

// This is a workaround to resolve correctly NAPI modules in the monorepo
// Real-world apps should just `import { createJazzSession } from "jazz-tools/backend"`
import { createRequire as createRequireFromModule } from "node:module";
const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const nodeRequire = createRequire(import.meta.url);

type BackendState = Awaited<ReturnType<typeof import("jazz-tools/backend").createJazzSession>>;
type BackendModule = {
  createJazzSession: (config: JazzSessionConfig) => Promise<BackendState>;
};
type GlobalBackendState = typeof globalThis & {
  __jazzNextCsrSsrBackend?: Promise<BackendState>;
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

export async function getBackendDb(): Promise<Db> {
  if (!globalState.__jazzNextCsrSsrBackend) {
    const config = readBackendConfig();
    const { createJazzSession } = nodeRequire("jazz-tools/backend") as BackendModule;
    globalState.__jazzNextCsrSsrBackend = createJazzSession({
      appId: config.appId,
      app: schemaApp,
      permissions,
      driver: { type: "memory" },
      serverUrl: config.serverUrl,
      initial: { backendSecret: config.backendSecret },
    });
  }
  const pending = globalState.__jazzNextCsrSsrBackend;
  try {
    const snapshot = (await pending).getSnapshot();
    if (snapshot.status !== "ready" || !snapshot.client) {
      throw snapshot.error ?? new Error("Backend session is not ready");
    }
    return snapshot.client.db;
  } catch (error) {
    if (globalState.__jazzNextCsrSsrBackend === pending) {
      globalState.__jazzNextCsrSsrBackend = undefined;
    }
    throw error;
  }
}
