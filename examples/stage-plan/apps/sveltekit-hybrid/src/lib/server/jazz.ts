import { createJazz, remote, sqlite, type Jazz } from "jazz-tools/backend";
import { app } from "../../../schema";
import permissions from "../../../permissions";
import { serverConfig } from "./config";

let pending: Promise<Jazz> | undefined;

/**
 * StagePlan's Jazz, inside this SvelteKit server: one per process, mounted
 * under `/jazz` by src/hooks.server.ts. Browsers sync with the app's own
 * origin, and the data lives in a local SQLite directory unless
 * JAZZ_SERVER_URL names a sync server.
 */
export function jazz(): Promise<Jazz> {
  if (pending) return pending;
  const config = serverConfig();
  const started = createJazz({
    appId: config.appId,
    app,
    permissions,
    store: config.remote ? remote(config.remote) : sqlite({ path: config.dataPath }),
    // Better Auth's session cookie decides who the user is. Guests have no
    // session and keep their local-first account.
    authenticate: async (request) => {
      const { auth } = await import("./auth");
      const session = await auth().api.getSession({ headers: request.headers });
      return session ? { userId: session.user.id } : null;
    },
  });
  pending = started;
  // A failed start must not poison every later request.
  started.catch(() => {
    if (pending === started) pending = undefined;
  });
  return started;
}
