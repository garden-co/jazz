import { app } from "@/schema";
import permissions from "@/permissions";
import { serverSecret } from "@/lib/server-secret";
import { JAZZ_ENV } from "@/lib/jazz-env";
import type { JazzClient } from "jazz-tools/backend";
import { createRequire as createRequireFromModule } from "node:module";

const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const nodeRequire = createRequire(import.meta.url);
// Load the backend through Node at runtime, never through the bundler: Turbopack
// bundles a static "jazz-tools/backend" import (and jazz-napi with it) despite
// serverExternalPackages, and then fails to resolve the native binding.
export const { createJazzBackend, resolveRequestSession } = nodeRequire(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

type AuthSession = Awaited<ReturnType<typeof createJazzBackend>>;

declare global {
  var __wequencerAuthSession: Promise<AuthSession> | undefined;
}

/** Share one session owner across concurrent auth/bootstrap calls and Next reloads. */
export async function authJazzClient(): Promise<JazzClient> {
  const pending = (globalThis.__wequencerAuthSession ??= createJazzBackend({
    app,
    permissions,
    appId: process.env.NEXT_PUBLIC_JAZZ_APP_ID!,
    driver: { type: "memory" },
    serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!,
    backendSecret: serverSecret("BACKEND_SECRET", "wequencer-development-backend-secret"),
    env: JAZZ_ENV,
  }));
  try {
    return await pending;
  } catch (error) {
    // A failed admission/open must not poison all later requests.
    if (globalThis.__wequencerAuthSession === pending) {
      globalThis.__wequencerAuthSession = undefined;
    }
    throw error;
  }
}
