import { app } from "@/schema";
import permissions from "@/permissions";
import type { Jazz } from "jazz-tools/backend";
import { createRequire as createRequireFromModule } from "node:module";
import { jazzConfig } from "@/lib/jazz-config";

const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
// Load the backend through Node at runtime, never through the bundler: Turbopack
// bundles a static "jazz-tools/backend" import (and jazz-napi with it) despite
// serverExternalPackages, and then fails to resolve the native binding.
const { createJazz, remote, sqlite } = createRequire(import.meta.url)(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

declare global {
  var __wequencerJazz: Promise<Jazz> | undefined;
}

/**
 * Wequencer's Jazz, inside this Next app, mounted under `/jazz` (see
 * app/jazz/[...path]/route.ts). Its `db` holds backend authority: Better Auth
 * persists its rows through it, and it writes profiles. Shared across
 * concurrent requests and Next hot reloads.
 */
export function jazz(): Promise<Jazz> {
  const config = jazzConfig();
  const pending = (globalThis.__wequencerJazz ??= createJazz({
    appId: config.appId,
    app,
    permissions,
    store: config.remote ? remote(config.remote) : sqlite({ path: config.dataPath! }),
    // Better Auth's session cookie decides who the bandmate is.
    authenticate: async (request) => {
      const { auth } = await import("@/lib/auth");
      const session = await auth.api.getSession({ headers: request.headers });
      return session ? { userId: session.user.id } : null;
    },
  }));
  // A failed start must not poison every later request.
  pending.catch(() => {
    if (globalThis.__wequencerJazz === pending) globalThis.__wequencerJazz = undefined;
  });
  return pending;
}
