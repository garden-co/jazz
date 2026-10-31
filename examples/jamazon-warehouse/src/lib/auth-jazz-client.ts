import permissions from "@/permissions";
import { app } from "@/schema";
import type { JazzClient } from "jazz-tools/backend";
import { createRequire as createRequireFromModule } from "node:module";
import { jazzAppId, jazzServerUrl, serverSecret } from "./config";
import { jazzEnv } from "./jazz-env";

const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const { createJazzBackend } = createRequire(import.meta.url)(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

type BackendSession = Awaited<ReturnType<typeof createJazzBackend>>;

declare global {
  var __jamazonBackendSession: Promise<BackendSession> | undefined;
}

/**
 * The server's own Jazz client, with backend authority. Better Auth stores its
 * rows through it and the bootstrap route seeds and staffs warehouses with it.
 * One session is shared across concurrent requests and Next reloads.
 */
export async function backendJazzClient(): Promise<JazzClient> {
  const pending = (globalThis.__jamazonBackendSession ??= createJazzBackend({
    app,
    permissions,
    appId: jazzAppId,
    driver: { type: "memory" },
    serverUrl: jazzServerUrl,
    backendSecret: serverSecret("BACKEND_SECRET"),
    env: jazzEnv,
  }));
  try {
    return await pending;
  } catch (error) {
    // A failed open must not poison later requests.
    if (globalThis.__jamazonBackendSession === pending) {
      globalThis.__jamazonBackendSession = undefined;
    }
    throw error;
  }
}
