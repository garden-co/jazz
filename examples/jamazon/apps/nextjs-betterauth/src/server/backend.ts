import type { Jazz } from "jazz-tools/backend";
import { createRequire as createRequireFromModule } from "node:module";
import { app } from "@/schema";
import permissions from "@/jazz.permissions.json" with { type: "json" };
import { serverConfig } from "./config";

// Load the native backend at runtime rather than through the Next bundler.
const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const { acceptDeviceAccounts, createJazz, remote, sqlite } = createRequire(import.meta.url)(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

declare global {
  var __jamazonJazz: Promise<Jazz> | undefined;
}

/**
 * The store's Jazz, inside this Next app, mounted under `/jazz` (see
 * app/jazz/[...path]/route.ts). Its `db` holds backend authority: it seeds the
 * catalogue, places orders, records payments and ships, and Better Auth
 * persists its rows through it. Shared across concurrent requests and Next
 * hot reloads.
 */
export function backend(): Promise<Jazz> {
  const { remote: sync } = serverConfig;
  const pending = (globalThis.__jamazonJazz ??= createJazz({
    app,
    permissions,
    store: sync
      ? remote({ ...sync, appId: serverConfig.appId })
      : sqlite({ path: serverConfig.dataPath }),
    // Better Auth's session cookie decides who the shopper is. Guests have
    // no Better Auth session: they stay on their device account.
    authenticate: async (request) => {
      const { auth } = await import("./auth");
      const session = await auth.api.getSession({ headers: request.headers });
      // Signed out: a device-account guest shopper.
      return session
        ? {
            userId: session.user.id,
            expiresAt: Math.floor(session.session.expiresAt.getTime() / 1000),
          }
        : acceptDeviceAccounts(request);
    },
  }).ready());
  // A failed start must not poison later requests.
  pending.catch(() => {
    if (globalThis.__jamazonJazz === pending) globalThis.__jamazonJazz = undefined;
  });
  return pending;
}
