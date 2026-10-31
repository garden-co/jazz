import { app } from "../../schema";
import permissions from "../../permissions";
import { jazzEnv } from "./jazz-env";
import type { JazzClient } from "jazz-tools/backend";
import { createRequire as createRequireFromModule } from "node:module";

const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const nodeRequire = createRequire(import.meta.url);
const { createJazzBackend } = nodeRequire(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

type AuthSession = Awaited<ReturnType<typeof createJazzBackend>>;

declare global {
  var __recordPlayerAuthJazzSession: Promise<AuthSession> | undefined;
}

/** Share one session owner across concurrent auth/bootstrap calls and Next reloads. */
export async function authJazzClient(): Promise<JazzClient> {
  // Fail closed: `withJazz` injects a generated secret in development; a
  // deployment must set its own.
  const backendSecret = process.env.BACKEND_SECRET;
  if (!backendSecret) throw new Error("BACKEND_SECRET must be set");
  const pending = (globalThis.__recordPlayerAuthJazzSession ??= createJazzBackend({
    app,
    permissions,
    appId: process.env.NEXT_PUBLIC_JAZZ_APP_ID!,
    driver: { type: "memory" },
    serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!,
    backendSecret,
    env: jazzEnv,
  }));
  try {
    return await pending;
  } catch (error) {
    // A failed admission/open must not poison all later requests.
    if (globalThis.__recordPlayerAuthJazzSession === pending) {
      globalThis.__recordPlayerAuthJazzSession = undefined;
    }
    throw error;
  }
}
