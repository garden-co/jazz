import { createJazz, remote, sqlite, type Jazz } from "jazz-tools/backend";
import { app } from "../schema";
import permissions from "../permissions";
import { serverConfig } from "./config";

let instance: Promise<Jazz> | undefined;

/** StagePlan's Jazz, inside this Hono server. Browsers sync with it under `/jazz`. */
export function jazz(): Promise<Jazz> {
  const config = serverConfig();
  const pending = (instance ??= createJazz({
    app,
    permissions,
    store: config.remote
      ? remote({ url: config.remote.url, appId: config.appId, secret: config.remote.adminSecret })
      : sqlite({ path: config.dataPath, appId: config.appId }),
    // Better Auth's session cookie decides who the user is.
    authenticate: async (request) => {
      const { auth } = await import("./auth");
      const session = await auth.api.getSession({ headers: request.headers });
      return session ? { userId: session.user.id } : null;
    },
  }));
  // A failed start must not poison every later request.
  pending.catch(() => {
    if (instance === pending) instance = undefined;
  });
  return pending;
}
