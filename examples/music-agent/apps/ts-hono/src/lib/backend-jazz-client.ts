import { app } from "@/schema";
import permissions from "@/permissions";
import { createJazz, remote, sqlite, type Jazz } from "jazz-tools/backend";
import { jazzAppId } from "./jazz-env";

let instance: Promise<Jazz> | undefined;

/**
 * MusicAgent's Jazz, inside this Hono server and mounted under `/jazz`: one
 * per server process, shared by auth, bootstrap and the agent runner. Its `db` holds backend authority.
 *
 * Data lives in local SQLite (`.jazz/`) unless JAZZ_SERVER_URL names a sync
 * server such as Jazz Cloud, reached with the app's secrets.
 */
export function backendJazzClient(): Promise<Jazz> {
  const url = process.env.JAZZ_SERVER_URL;
  const pending = (instance ??= createJazz({
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
    if (instance === pending) instance = undefined;
  });
  return pending;
}

function requiredSecret(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} must be set when JAZZ_SERVER_URL is`);
  return value;
}
