import { app } from "@/schema";
import type { JazzClient } from "jazz-tools/backend";
import { createRequire as createRequireFromModule } from "node:module";
import { configuredIssuer } from "./identity";
import { serverSecret } from "./server-secret";

const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const { createJazzSession } = createRequire(import.meta.url)(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

type AuthSession = Awaited<ReturnType<typeof createJazzSession>>;

declare global {
  var __posterShopAuthSession: Promise<AuthSession> | undefined;
}

/** Share one session owner across concurrent auth/bootstrap calls and Next reloads. */
export async function authJazzClient(): Promise<JazzClient> {
  const pending = (globalThis.__posterShopAuthSession ??= createJazzSession({
    app,
    // These explicit local defaults let Next evaluate auth routes during a
    // bare production build. Deployments replace both public values.
    appId: process.env.NEXT_PUBLIC_JAZZ_APP_ID ?? "poster-shop-local",
    driver: { type: "memory" },
    serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL ?? "http://127.0.0.1:4200",
    initial: {
      backendSecret: serverSecret("BACKEND_SECRET", "poster-shop-development-backend-secret"),
    },
    env: configuredIssuer === "http://127.0.0.1:3000" ? "dev" : "prod",
    tier: "global",
  }));
  try {
    const session = await pending;
    const snapshot = session.getSnapshot();
    if (snapshot.status !== "ready" || !snapshot.client) {
      await session.close();
      throw snapshot.error ?? new Error("Backend session is not ready");
    }
    return snapshot.client;
  } catch (error) {
    // A failed admission/open must not poison all later requests.
    if (globalThis.__posterShopAuthSession === pending) {
      globalThis.__posterShopAuthSession = undefined;
    }
    throw error;
  }
}
