import type { JazzContext } from "jazz-tools/backend";

// This is a workaround to resolve correctly NAPI modules in the monorepo
// Real-world apps should just `import { createJazzContext } from "jazz-tools/backend"`
import { createRequire as createRequireFromModule } from "node:module";
const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const nodeRequire = createRequire(import.meta.url);
const { createJazzContext } = nodeRequire(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

declare global {
  var __authBetterAuthChatJazzContext: JazzContext | undefined;
}

function createAuthJazzContext() {
  return createJazzContext({
    appId: process.env.NEXT_PUBLIC_APP_ID!,
    driver: { type: "memory" },
    serverUrl: process.env.NEXT_PUBLIC_SYNC_SERVER_URL!,
    env: process.env.NODE_ENV === "production" ? "prod" : "dev",
    userBranch: "main",
    backendSecret: process.env.BACKEND_SECRET!,
    tier: "worker",
  });
}

export const authJazzContext =
  globalThis.__authBetterAuthChatJazzContext ?? createAuthJazzContext();

if (process.env.NODE_ENV !== "production") {
  globalThis.__authBetterAuthChatJazzContext = authJazzContext;
}
