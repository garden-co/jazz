import { app } from "../../schema";
import permissions from "../../permissions";
import { assertConfiguration } from "./config.mjs";
import type { Jazz } from "jazz-tools/backend";
import { createRequire as createRequireFromModule } from "node:module";

// Keep the NAPI load on the server, resolved from this app (a monorepo
// workaround: real apps just `import { createJazz } from "jazz-tools/backend"`).
const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const nodeRequire = createRequire(import.meta.url);
const { createJazz, memory, remote, sqlite } = nodeRequire(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

declare global {
  var __bandChatJazz: Promise<Jazz> | undefined;
}

/**
 * BandChat's Jazz, inside this Next app: one per process, shared across Next's
 * module graphs and reloads. Browsers sync with it under `/jazz`.
 */
export function jazz(): Promise<Jazz> {
  // Throws (fails closed) on an incomplete configuration.
  const config = assertConfiguration();
  const pending = (globalThis.__bandChatJazz ??= createJazz({
    appId: config.appId,
    app,
    permissions,
    store: config.remote
      ? remote({
          url: config.remote.url,
          adminSecret: config.remote.adminSecret,
          backendSecret: config.remote.backendSecret,
        })
      : process.env.NODE_ENV === "test"
        ? memory()
        : sqlite({ path: config.dataPath }),
    // Better Auth's session cookie decides who the user is.
    authenticate: async (request) => {
      const { auth } = await import("./auth");
      const session = await auth.api.getSession({ headers: request.headers });
      return session ? { userId: session.user.id } : null;
    },
  }));
  // A failed start must not poison every later request.
  pending.catch(() => {
    if (globalThis.__bandChatJazz === pending) globalThis.__bandChatJazz = undefined;
  });
  return pending;
}
