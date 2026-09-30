import { app } from "@/schema";
import permissions from "@/permissions";
import { serverSecret } from "@/lib/server-secret";
import { JAZZ_ENV } from "@/lib/jazz-env";
import type { JazzClient } from "jazz-tools/backend";
import { jazzBackend } from "@/lib/jazz-backend";

const { createJazzSession } = jazzBackend;

type AuthSession = Awaited<ReturnType<typeof createJazzSession>>;

declare global {
  var __wequencerAuthSession: Promise<AuthSession> | undefined;
}

/** Share one session owner across concurrent auth/bootstrap calls and Next reloads. */
export async function authJazzClient(): Promise<JazzClient> {
  const pending = (globalThis.__wequencerAuthSession ??= createJazzSession({
    app,
    permissions,
    appId: process.env.NEXT_PUBLIC_JAZZ_APP_ID!,
    driver: { type: "memory" },
    serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!,
    initial: {
      backendSecret: serverSecret("BACKEND_SECRET", "wequencer-development-backend-secret"),
    },
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
    if (globalThis.__wequencerAuthSession === pending) {
      globalThis.__wequencerAuthSession = undefined;
    }
    throw error;
  }
}
