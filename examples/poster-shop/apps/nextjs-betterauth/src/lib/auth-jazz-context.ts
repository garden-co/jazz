import type { JazzContext } from "jazz-tools/backend";
import { createRequire as createRequireFromModule } from "node:module";

const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const { createJazzContext } = createRequire(import.meta.url)(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

declare global {
  var __posterShopAuthContext: JazzContext | undefined;
}

export function authJazzContext(): JazzContext {
  if (!globalThis.__posterShopAuthContext) {
    globalThis.__posterShopAuthContext = createJazzContext({
      appId: process.env.NEXT_PUBLIC_JAZZ_APP_ID!,
      driver: { type: "memory" },
      serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!,
      backendSecret: process.env.BACKEND_SECRET ?? "poster-shop-development-backend-secret",
      env: process.env.NODE_ENV === "production" ? "prod" : "dev",
      tier: "global",
    });
  }
  return globalThis.__posterShopAuthContext;
}
