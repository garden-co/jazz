import { createJazz, memory, remote, sqlite, type Jazz } from "jazz-tools/backend";
import { app } from "../schema";
import permissions from "../permissions";
import { assertConfiguration } from "../src/lib/config.mjs";

let instance: Promise<Jazz> | undefined;

/** BandChat's Jazz, inside this Hono server. Browsers sync with it under `/jazz`. */
export function jazz(): Promise<Jazz> {
  // Throws (fails closed) on an incomplete configuration.
  const config = assertConfiguration();
  const pending = (instance ??= createJazz({
    app,
    permissions,
    store: config.remote
      ? remote({ url: config.remote.url, appId: config.appId, secret: config.remote.adminSecret })
      : process.env.NODE_ENV === "test"
        ? memory({ appId: config.appId })
        : sqlite({ path: config.dataPath, appId: config.appId }),
    // Better Auth's session cookie decides who the user is.
    authenticate: async (request) => {
      const { auth } = await import("./auth");
      const session = await auth.api.getSession({ headers: request.headers });
      return session ? { userId: session.user.id } : null;
    },
  }).ready());
  // A failed start must not poison every later request.
  pending.catch(() => {
    if (instance === pending) instance = undefined;
  });
  return pending;
}
