import type { JazzClient } from "jazz-tools/backend";
import { createRequire as createRequireFromModule } from "node:module";
import { app } from "@/schema";
import permissions from "@/permissions";
import { jazzEnv } from "@/src/lib/jazz-env";
import { serverConfig } from "./config";

// Load the native backend at runtime rather than through the Next bundler.
const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const { createJazzBackend } = createRequire(import.meta.url)(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

type BackendSession = Awaited<ReturnType<typeof createJazzBackend>>;

declare global {
  var __jamazonBackend: Promise<BackendSession> | undefined;
}

/**
 * The store's single backend client. It holds backend authority: it seeds the
 * catalogue, places orders, records payments and ships. Better Auth also
 * persists its rows through it. Shared across concurrent requests and Next
 * hot reloads.
 */
export async function backend(): Promise<JazzClient> {
  const pending = (globalThis.__jamazonBackend ??= createJazzBackend({
    app,
    permissions,
    appId: serverConfig.appId,
    driver: { type: "memory" },
    serverUrl: serverConfig.serverUrl,
    backendSecret: serverConfig.backendSecret,
    env: jazzEnv,
    jwtAudience: serverConfig.origin,
  }));
  try {
    return await pending;
  } catch (error) {
    // A failed admission must not poison later requests.
    if (globalThis.__jamazonBackend === pending) globalThis.__jamazonBackend = undefined;
    throw error;
  }
}
