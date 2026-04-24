import type { JazzContext } from "jazz-tools/backend";

// Workaround to resolve NAPI modules correctly in the monorepo. Real-world
// apps can just `import { createJazzContext } from "jazz-tools/backend"`.
import { createRequire as createRequireFromModule } from "node:module";
const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const nodeRequire = createRequire(import.meta.url);
const { createJazzContext } = nodeRequire(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

declare global {
  var __sveltekitBetterAuthJazzContext: JazzContext | undefined;
}

function create() {
  return createJazzContext({
    appId: process.env.PUBLIC_JAZZ_APP_ID!,
    driver: { type: "memory" },
    serverUrl: process.env.PUBLIC_JAZZ_SERVER_URL!,
    env: process.env.NODE_ENV === "production" ? "prod" : "dev",
    userBranch: "main",
    backendSecret: process.env.BACKEND_SECRET!,
    tier: "global",
  });
}

export function authJazzContext(): JazzContext {
  const existing = globalThis.__sveltekitBetterAuthJazzContext;
  if (existing) return existing;
  const ctx = create();
  globalThis.__sveltekitBetterAuthJazzContext = ctx;
  return ctx;
}
