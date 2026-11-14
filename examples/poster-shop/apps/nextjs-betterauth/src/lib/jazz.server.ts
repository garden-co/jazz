import { app } from "@/schema";
import permissions from "@/jazz.permissions.json" with { type: "json" };
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
  var __posterShopJazz: Promise<Jazz> | undefined;
}

/**
 * PosterShop's Jazz, inside this Next app, mounted under `/jazz` (see
 * app/jazz/[...path]/route.ts). Its `db` holds backend authority: Better Auth
 * persists its rows through it, and the API routes seed posters and redeem
 * invites with it. Shared across concurrent requests and Next hot reloads.
 */
export function jazz(): Promise<Jazz> {
  // Throws (fails closed) on an incomplete configuration.
  const config = assertBuildConfiguration();
  const sync = config.remote;
  const pending = (globalThis.__posterShopJazz ??= createJazz({
    app,
    permissions,
    store: sync
      ? // assertBuildConfiguration checked that the admin secret is set.
        remote({ url: sync.url, appId: config.appId, secret: sync.adminSecret! })
      : sqlite({ path: config.dataPath }),
    // Better Auth's session cookie decides who the user is. The display name
    // rides along as a claim, for the first poster's title.
    authenticate: async (request) => {
      const { auth } = await import("./auth");
      const session = await auth.api.getSession({ headers: request.headers });
      return session
        ? {
            userId: session.user.id,
            expiresAt: Math.floor(session.session.expiresAt.getTime() / 1000),
            claims: { name: session.user.name },
          }
        : null;
    },
  }).ready());
  // A failed start must not poison every later request.
  pending.catch(() => {
    if (globalThis.__posterShopJazz === pending) globalThis.__posterShopJazz = undefined;
  });
  return pending;
}
