import { app } from "../../schema";
import permissions from "../../permissions";
import { assertConfiguration, jazzServer } from "./config.mjs";
import { JAZZ_ENV } from "./jazz-env";
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
  // Throws (fails closed) when the app, sync server or backend secret is unset.
  const { backendSecret } = assertConfiguration();
  const { appId, serverUrl } = jazzServer();
  const pending = (globalThis.__bandChatAuthSession ??= createJazzSession({
    app,
    permissions,
    appId,
    driver: { type: "memory" },
    serverUrl,
    initial: { backendSecret },
    env: JAZZ_ENV,
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
    if (globalThis.__bandChatAuthSession === pending) {
      globalThis.__bandChatAuthSession = undefined;
    }
    throw error;
  }
}
