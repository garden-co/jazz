import "server-only";
import { app } from "@/schema";
import permissions from "@/permissions";
import type { Jazz } from "jazz-tools/backend";
import { createRequire as createRequireFromModule } from "node:module";
import { jazzAppId } from "./jazz-env";

// Load the native backend at runtime, resolved from this app, rather than
// through the Next bundler (a monorepo workaround: real apps just
// `import { createJazz } from "jazz-tools/backend"`).
const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const { createJazz, remote, sqlite } = createRequire(import.meta.url)(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

declare global {
  var __musicAgentJazz: Promise<Jazz> | undefined;
}

/**
 * MusicAgent's Jazz, inside this Next app and mounted under `/jazz`: one per
 * server process, shared by auth, bootstrap and the agent runner, and kept
 * across Next dev reloads. Its `db` holds backend authority.
 *
 * Data lives in local SQLite (`.jazz/`) unless JAZZ_SERVER_URL names a sync
 * server such as Jazz Cloud, reached with the app's secrets.
 */
export function backendJazzClient(): Promise<Jazz> {
  const url = process.env.JAZZ_SERVER_URL;
  const pending = (globalThis.__musicAgentJazz ??= createJazz({
    appId: jazzAppId,
    app,
    permissions,
    store: url
      ? remote({
          url,
          adminSecret: requiredSecret("JAZZ_ADMIN_SECRET"),
          backendSecret: requiredSecret("BACKEND_SECRET"),
        })
      : sqlite({ path: process.env.JAZZ_DATA_PATH || ".jazz" }),
    // Better Auth's session cookie decides who the user is.
    authenticate: async (request) => {
      const { auth } = await import("./auth");
      const session = await auth.api.getSession({ headers: request.headers });
      return session ? { userId: session.user.id } : null;
    },
  }));
  // A failed start must not poison all later requests.
  pending.catch(() => {
    if (globalThis.__musicAgentJazz === pending) globalThis.__musicAgentJazz = undefined;
  });
  return pending;
}

function requiredSecret(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} must be set when JAZZ_SERVER_URL is`);
  return value;
}
