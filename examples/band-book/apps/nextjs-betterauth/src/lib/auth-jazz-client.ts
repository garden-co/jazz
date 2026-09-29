import { app } from "@/schema";
import permissions from "@/permissions";
import type { JazzClient } from "jazz-tools/backend";
import { createRequire as createRequireFromModule } from "node:module";
import { jazzAppId, jazzEnv, jazzServerUrl } from "./config";
import { serverSecret } from "./server-secret";

const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const { createJazzSession } = createRequire(import.meta.url)(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

type AuthSession = Awaited<ReturnType<typeof createJazzSession>>;

declare global {
  var __bandBookAuthSession: Promise<AuthSession> | undefined;
}

/** Share one session owner across concurrent auth/bootstrap calls and Next reloads. */
export async function authJazzClient(): Promise<JazzClient> {
  const pending = (globalThis.__bandBookAuthSession ??= createJazzSession({
    app,
    permissions,
    appId: jazzAppId,
    driver: { type: "memory" },
    serverUrl: jazzServerUrl,
    initial: {
      backendSecret: serverSecret("BACKEND_SECRET", "band-book-development-backend-secret"),
    },
    env: jazzEnv,
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
    if (globalThis.__bandBookAuthSession === pending) {
      globalThis.__bandBookAuthSession = undefined;
    }
    throw error;
  }
}
