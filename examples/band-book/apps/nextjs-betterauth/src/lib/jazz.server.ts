import { app } from "@/schema";
import permissions from "@/permissions";
import type { Jazz } from "jazz-tools/backend";
import { createRequire as createRequireFromModule } from "node:module";
import { assertBuildConfiguration } from "./build-config.mjs";

// Keep the NAPI load on the server, resolved from this app (a monorepo
// workaround: real apps just `import { createJazz } from "jazz-tools/backend"`).
const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const { createJazz, remote, sqlite } = createRequire(import.meta.url)(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

declare global {
  var __bandBookJazz: Promise<Jazz> | undefined;
}

/**
 * BandBook's Jazz, inside this Next app, mounted under `/jazz` (see
 * app/jazz/[...path]/route.ts). Its `db` holds backend authority: Better Auth
 * persists its rows through it, and the bootstrap and invite routes write
 * with it. Shared across concurrent requests and Next hot reloads.
 */
export function jazz(): Promise<Jazz> {
  // Throws (fails closed) on an incomplete configuration.
  const config = assertBuildConfiguration();
  const sync = config.remote;
  const pending = (globalThis.__bandBookJazz ??= createJazz({
    appId: config.appId,
    app,
    permissions,
    store: sync
      ? // assertBuildConfiguration checked that both secrets are set.
        remote({
          url: sync.url,
          adminSecret: sync.adminSecret!,
          backendSecret: sync.backendSecret!,
        })
      : sqlite({ path: config.dataPath }),
    // Better Auth's session cookie decides who the band member is.
    authenticate: async (request) => {
      const { auth } = await import("./auth");
      const session = await auth.api.getSession({ headers: request.headers });
      return session ? { userId: session.user.id } : null;
    },
  }));
  // A failed start must not poison every later request.
  pending.catch(() => {
    if (globalThis.__bandBookJazz === pending) globalThis.__bandBookJazz = undefined;
  });
  return pending;
}
