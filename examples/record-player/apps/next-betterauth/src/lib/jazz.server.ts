import { app } from "../../schema";
import permissions from "../../permissions";
import type { Jazz } from "jazz-tools/backend";
import { createRequire as createRequireFromModule } from "node:module";
import { readConfig } from "./config.mjs";

// Keep the NAPI load on the server, resolved from this app (a monorepo
// workaround: real apps just `import { createJazz } from "jazz-tools/backend"`).
const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const { createJazz, remote, sqlite } = createRequire(import.meta.url)(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

declare global {
  var __recordPlayerJazz: Promise<Jazz> | undefined;
}

/**
 * RecordPlayer's Jazz, inside this Next app, mounted under `/jazz` (see
 * app/jazz/[...path]/route.ts). Better Auth persists its rows through its
 * backend `db`. Shared across concurrent requests and Next hot reloads.
 */
export function jazz(): Promise<Jazz> {
  // Throws (fails closed) on an incomplete configuration.
  const config = readConfig();
  const pending = (globalThis.__recordPlayerJazz ??= createJazz({
    appId: config.appId,
    app,
    permissions,
    store: config.remote ? remote(config.remote) : sqlite({ path: config.dataPath }),
    // Better Auth's session cookie decides who the listener is.
    authenticate: async (request) => {
      const { auth } = await import("./auth");
      const session = await auth.api.getSession({ headers: request.headers });
      return session ? { userId: session.user.id } : null;
    },
  }));
  // A failed start must not poison every later request.
  pending.catch(() => {
    if (globalThis.__recordPlayerJazz === pending) globalThis.__recordPlayerJazz = undefined;
  });
  return pending;
}
