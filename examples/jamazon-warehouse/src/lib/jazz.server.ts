import permissions from "@/permissions";
import { app } from "@/schema";
import type { Jazz } from "jazz-tools/backend";
import { createRequire as createRequireFromModule } from "node:module";
import { jazzAppId, jazzStore } from "./config";

// Keep the NAPI load on the server, resolved from this app (a monorepo
// workaround: real apps just `import { createJazz } from "jazz-tools/backend"`).
const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const { createJazz, remote, sqlite } = createRequire(import.meta.url)(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

declare global {
  var __jamazonWarehouseJazz: Promise<Jazz> | undefined;
}

/**
 * The warehouse's Jazz, inside this Next app, mounted under `/jazz` (see
 * app/jazz/[...path]/route.ts). Its `db` holds backend authority: Better Auth
 * stores its rows through it and the bootstrap route seeds and staffs
 * warehouses with it. Shared across concurrent requests and Next reloads.
 */
export function jazz(): Promise<Jazz> {
  const store = jazzStore();
  const pending = (globalThis.__jamazonWarehouseJazz ??= createJazz({
    appId: jazzAppId,
    app,
    permissions,
    store: store.kind === "remote" ? remote(store) : sqlite({ path: store.path }),
    // Better Auth's session cookie decides who the operator is.
    authenticate: async (request) => {
      const { auth } = await import("./auth");
      const session = await auth.api.getSession({ headers: request.headers });
      return session ? { userId: session.user.id } : null;
    },
  }));
  // A failed start must not poison later requests.
  pending.catch(() => {
    if (globalThis.__jamazonWarehouseJazz === pending)
      globalThis.__jamazonWarehouseJazz = undefined;
  });
  return pending;
}
