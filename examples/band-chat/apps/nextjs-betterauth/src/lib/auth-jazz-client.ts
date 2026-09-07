import { app } from "../../schema";
import type { JazzClient } from "jazz-tools/backend";
import { createRequire as createRequireFromModule } from "node:module";

// Keep the NAPI load on the server. This mirrors the maintained Better Auth
// example while avoiding a module-evaluation failure during Next page builds.
const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const nodeRequire = createRequire(import.meta.url);
const { createJazzSession } = nodeRequire(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

type AuthSession = Awaited<ReturnType<typeof createJazzSession>>;

declare global {
  var __bandChatAuthSession: Promise<AuthSession> | undefined;
}

/** Share one session owner across concurrent auth/bootstrap calls and Next reloads. */
export async function authJazzClient(): Promise<JazzClient> {
  const pending = (globalThis.__bandChatAuthSession ??= createJazzSession({
    app,
    appId: process.env.NEXT_PUBLIC_JAZZ_APP_ID!,
    driver: { type: "memory" },
    serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!,
    initial: {
      backendSecret: process.env.BACKEND_SECRET ?? "band-chat-development-backend-secret",
    },
    env: process.env.NODE_ENV === "production" ? "prod" : "dev",
    tier: "global",
  }));
  try {
    const session = await pending;
    const snapshot = session.getSnapshot();
    if (snapshot.status !== "ready" || !snapshot.client) {
      throw snapshot.error ?? new Error("Backend session is not ready");
    }
    return snapshot.client;
  } catch (error) {
    // A failed admission/open must not poison all later requests.
    if (globalThis.__bandChatAuthSession === pending) {
      globalThis.__bandChatAuthSession = undefined;
    }
    throw error;
  }
}
