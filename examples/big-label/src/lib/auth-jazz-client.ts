import { app } from "../../schema";
import type { JazzClient } from "jazz-tools/backend";
import { createRequire as createRequireFromModule } from "node:module";
import { serverSecret } from "./server-secret";

// Keep the N-API backend runtime external to Next/Turbopack. This mirrors how
// an app consumes the published package, rather than bundling a platform .node
// asset into a route chunk.
const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const { createJazzSession } = createRequire(import.meta.url)(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

type AuthSession = Awaited<ReturnType<typeof createJazzSession>>;

declare global {
  var __bigLabelAuthSession: Promise<AuthSession> | undefined;
}

/** Share one session owner across concurrent auth/bootstrap calls and Next reloads. */
export async function authJazzClient(): Promise<JazzClient> {
  const pending = (globalThis.__bigLabelAuthSession ??= createJazzSession({
    app,
    appId: process.env.NEXT_PUBLIC_JAZZ_APP_ID!,
    driver: { type: "memory" },
    serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!,
    initial: { backendSecret: serverSecret("BACKEND_SECRET", "big-label-dev-backend") },
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
    if (globalThis.__bigLabelAuthSession === pending) {
      globalThis.__bigLabelAuthSession = undefined;
    }
    throw error;
  }
}
