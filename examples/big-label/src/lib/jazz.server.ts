import { app } from "../../schema";
import permissions from "../../permissions";
import type { Jazz } from "jazz-tools/backend";
import { createRequire as createRequireFromModule } from "node:module";
import { jazzConfig } from "./jazz-config";

// Keep the N-API backend runtime external to Next/Turbopack. This mirrors how
// an app consumes the published package, rather than bundling a platform .node
// asset into a route chunk.
const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const { createJazz, memory, remote, sqlite } = createRequire(import.meta.url)(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

declare global {
  var __bigLabelJazz: Promise<Jazz> | undefined;
}

/**
 * BigLabel's Jazz, inside this Next app, mounted under `/jazz` (see
 * app/jazz/[...path]/route.ts). Its `db` holds backend authority: Better Auth
 * persists its rows through it, and the API routes bootstrap labels with it.
 * `forRequest` reads as the signed-in user, under their permissions. Shared
 * across concurrent requests and Next hot reloads.
 */
export function jazz(): Promise<Jazz> {
  const config = jazzConfig();
  const pending = (globalThis.__bigLabelJazz ??= createJazz({
    appId: config.appId,
    app,
    permissions,
    store: config.remote
      ? remote(config.remote)
      : process.env.NODE_ENV === "test"
        ? memory()
        : sqlite({ path: config.dataPath! }),
    // Better Auth's session cookie decides who the user is.
    authenticate: async (request) => {
      const { auth } = await import("./auth");
      const session = await auth.api.getSession({ headers: request.headers });
      return session ? { userId: session.user.id } : null;
    },
  }));
  // A failed start must not poison every later request.
  pending.catch(() => {
    if (globalThis.__bigLabelJazz === pending) globalThis.__bigLabelJazz = undefined;
  });
  return pending;
}
