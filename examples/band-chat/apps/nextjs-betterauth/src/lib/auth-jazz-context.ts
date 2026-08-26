import type { JazzContext } from "jazz-tools/backend";
import { createRequire as createRequireFromModule } from "node:module";

// Keep the NAPI load on the server. This mirrors the maintained Better Auth
// example while avoiding a module-evaluation failure during Next page builds.
const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const nodeRequire = createRequire(import.meta.url);
const { createJazzContext } = nodeRequire(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

declare global {
  var __bandChatAuthContext: JazzContext | undefined;
}

export function authJazzContext(): JazzContext {
  const existing = globalThis.__bandChatAuthContext;
  if (existing) return existing;

  const context = createJazzContext({
    appId: process.env.NEXT_PUBLIC_JAZZ_APP_ID!,
    driver: { type: "memory" },
    serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!,
    backendSecret: process.env.BACKEND_SECRET ?? "band-chat-development-backend-secret",
    env: process.env.NODE_ENV === "production" ? "prod" : "dev",
    tier: "global",
  });
  globalThis.__bandChatAuthContext = context;
  return context;
}
