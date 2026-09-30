import { app } from "../../schema";
import permissions from "../../permissions";
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

const appOrigin = () => process.env.NEXT_PUBLIC_APP_ORIGIN ?? "http://127.0.0.1:3000";

type AuthSession = Awaited<ReturnType<typeof createJazzSession>>;

declare global {
  var __bigLabelAuthSession: Promise<AuthSession> | undefined;
}

/** Share one session owner across concurrent auth/bootstrap calls and Next reloads. */
export async function authJazzClient(): Promise<JazzClient> {
  const pending = (globalThis.__bigLabelAuthSession ??= createJazzSession({
    app,
    permissions,
    appId: process.env.NEXT_PUBLIC_JAZZ_APP_ID!,
    driver: { type: "memory" },
    serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!,
    // Lets `forRequest()` verify a browser's Better Auth JWT, so a route can
    // write as that user under the same permissions the browser has.
    jwksUrl: `${appOrigin()}/api/auth/jwks`,
    jwtIssuer: appOrigin(),
    jwtAudience: appOrigin(),
    initial: { backendSecret: serverSecret("BACKEND_SECRET", "big-label-dev-backend") },
    env: process.env.NODE_ENV === "production" ? "prod" : "dev",
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
    if (globalThis.__bigLabelAuthSession === pending) {
      globalThis.__bigLabelAuthSession = undefined;
    }
    throw error;
  }
}
