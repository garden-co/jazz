import type { JazzContext } from "jazz-tools/backend";
import { createRequire as createRequireFromModule } from "node:module";
import { configuredIssuer } from "./identity";

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
      // These explicit local defaults let Next evaluate auth routes during a
      // bare production build. Deployments replace both public values.
      appId: process.env.NEXT_PUBLIC_JAZZ_APP_ID ?? "poster-shop-local",
      driver: { type: "memory" },
      serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL ?? "http://127.0.0.1:4200",
      backendSecret: process.env.BACKEND_SECRET ?? "poster-shop-development-backend-secret",
      env: configuredIssuer === "http://127.0.0.1:3000" ? "dev" : "prod",
      tier: "global",
    });
  }
  return globalThis.__posterShopAuthContext;
}
