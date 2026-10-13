import "server-only";
import { app } from "@/schema";
import permissions from "@/permissions";
import { createJazzSession, type JazzClient } from "jazz-tools/backend";
import { appOrigin } from "./app-origin";
import { jazzAppId, jazzEnv, jazzServerUrl } from "./jazz-env";
import { serverSecret } from "./server-secret";

type BackendSession = Awaited<ReturnType<typeof createJazzSession>>;

declare global {
  var __musicAgentBackendSession: Promise<BackendSession> | undefined;
}

/** One backend-authority session per server process, shared by auth, bootstrap
 * and the agent runner, and kept across Next dev reloads. */
export async function backendJazzClient(): Promise<JazzClient> {
  const pending = (globalThis.__musicAgentBackendSession ??= createJazzSession({
    app,
    permissions,
    appId: jazzAppId,
    driver: { type: "memory" },
    serverUrl: jazzServerUrl,
    // forRequest() verifies the user's app JWT with these, then reads as that user.
    jwksUrl: `${appOrigin}/api/auth/jwks`,
    jwtIssuer: appOrigin,
    jwtAudience: appOrigin,
    initial: {
      backendSecret: serverSecret("BACKEND_SECRET", "music-agent-development-backend-secret"),
    },
    env: jazzEnv,
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
    if (globalThis.__musicAgentBackendSession === pending) {
      globalThis.__musicAgentBackendSession = undefined;
    }
    throw error;
  }
}
