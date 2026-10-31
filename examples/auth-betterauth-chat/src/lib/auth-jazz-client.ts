import { app } from "../../schema";
import permissions from "../../permissions";
import type { JazzClient } from "jazz-tools/backend";

// This is a workaround to resolve correctly NAPI modules in the monorepo
// Real-world apps should just `import { createJazzBackend } from "jazz-tools/backend"`
import { createRequire as createRequireFromModule } from "node:module";
const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const nodeRequire = createRequire(import.meta.url);
const { createJazzBackend } = nodeRequire(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

type AuthSession = Awaited<ReturnType<typeof createJazzBackend>>;

declare global {
  var __authBetterAuthChatJazzSession: Promise<AuthSession> | undefined;
}

/** Share one session owner across concurrent auth/bootstrap calls and Next reloads. */
export async function authJazzClient(): Promise<JazzClient> {
  const pending = (globalThis.__authBetterAuthChatJazzSession ??= createJazzBackend({
    app,
    permissions,
    appId: process.env.NEXT_PUBLIC_JAZZ_APP_ID!,
    driver: { type: "memory" },
    serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!,
    env: process.env.NODE_ENV === "production" ? "prod" : "dev",
    backendSecret: process.env.BACKEND_SECRET!,
  }));
  try {
    return await pending;
  } catch (error) {
    // A failed admission/open must not poison all later requests.
    if (globalThis.__authBetterAuthChatJazzSession === pending) {
      globalThis.__authBetterAuthChatJazzSession = undefined;
    }
    throw error;
  }
}
