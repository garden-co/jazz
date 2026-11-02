import { app } from "../../schema";
import permissions from "../../permissions";
import type { Jazz } from "jazz-tools/backend";

// This is a workaround to resolve correctly NAPI modules in the monorepo
// Real-world apps should just `import { createJazz } from "jazz-tools/backend"`
import { createRequire as createRequireFromModule } from "node:module";
const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const nodeRequire = createRequire(import.meta.url);
const { createJazz, memory, sqlite } = nodeRequire(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

declare global {
  var __authBetterAuthChatJazz: Promise<Jazz> | undefined;
}

/** One Jazz per process, shared across Next's module graphs and reloads. */
export function jazz(): Promise<Jazz> {
  const pending = (globalThis.__authBetterAuthChatJazz ??= createJazz({
    appId: process.env.NEXT_PUBLIC_JAZZ_APP_ID!,
    app,
    permissions,
    store: process.env.NODE_ENV === "test" ? memory() : sqlite({ path: ".jazz" }),
    // Better Auth's session cookie decides who the user is; its `role`
    // becomes the claim the chat permissions check.
    authenticate: async (request) => {
      const { auth } = await import("./auth");
      const session = await (await auth).api.getSession({ headers: request.headers });
      if (!session) return null;
      const role = (session.user as { role?: string | string[] | null }).role;
      return {
        userId: session.user.id,
        claims: { role: Array.isArray(role) ? role[0] : (role ?? "") },
      };
    },
  }));
  // A failed start must not poison every later request.
  pending.catch(() => {
    if (globalThis.__authBetterAuthChatJazz === pending)
      globalThis.__authBetterAuthChatJazz = undefined;
  });
  return pending;
}
