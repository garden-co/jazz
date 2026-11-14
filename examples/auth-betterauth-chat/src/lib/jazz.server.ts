import { app } from "../../schema";
import permissions from "../../permissions";
import type { Jazz } from "jazz-tools/backend";

// This is a workaround to resolve correctly NAPI modules in the monorepo
// Real-world apps should just `import { createJazz } from "jazz-tools/backend"`
import { createRequire as createRequireFromModule } from "node:module";
const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const nodeRequire = createRequire(import.meta.url);
const { acceptDeviceAccounts, createJazz, memory, sqlite } = nodeRequire(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

declare global {
  var __authBetterAuthChatJazz: Promise<Jazz> | undefined;
}

// #region default-stack-create-jazz
/** One Jazz per process, shared across Next's module graphs and reloads. */
export function jazz(): Promise<Jazz> {
  const pending = (globalThis.__authBetterAuthChatJazz ??= createJazz({
    app,
    permissions,
    store: process.env.NODE_ENV === "test" ? memory() : sqlite({ path: ".jazz" }),
    // Better Auth's session cookie decides who the user is; its `role`
    // becomes the claim the chat permissions check.
    authenticate: async (request) => {
      const { auth } = await import("./auth");
      const session = await (await auth).api.getSession({ headers: request.headers });
      // Signed out: a guest on a device account, until sign-up links it.
      if (!session) return acceptDeviceAccounts(request);
      const role = (session.user as { role?: string | string[] | null }).role;
      return {
        userId: session.user.id,
        expiresAt: Math.floor(session.session.expiresAt.getTime() / 1000),
        claims: { role: Array.isArray(role) ? role[0] : (role ?? "") },
      };
    },
  }).ready());
  // A failed start must not poison every later request.
  pending.catch(() => {
    if (globalThis.__authBetterAuthChatJazz === pending)
      globalThis.__authBetterAuthChatJazz = undefined;
  });
  return pending;
}
// #endregion default-stack-create-jazz
