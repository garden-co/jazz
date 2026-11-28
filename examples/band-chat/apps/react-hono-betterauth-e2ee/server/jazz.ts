import { createJazz, memory, remote, sqlite, type Jazz } from "jazz-tools/backend";
import { app } from "../jazz.generated";
import permissions from "../jazz.permissions.json" with { type: "json" };
import { assertConfiguration } from "../src/lib/config.mjs";

let instance: Promise<Jazz> | undefined;

/**
 * BandChat's Jazz, inside this Hono server. Browsers sync with it under
 * `/jazz`. It holds no encryption keys: it relays and stores the encrypted
 * columns as ciphertext and reads only the plain ones.
 */
// #region bandchat-server-jazz
export function jazz(): Promise<Jazz> {
  // Throws (fails closed) on an incomplete configuration.
  const config = assertConfiguration();
  const pending = (instance ??= createJazz({
    app,
    permissions,
    store: config.remote
      ? remote({ url: config.remote.url, appId: config.appId, secret: config.remote.adminSecret })
      : process.env.NODE_ENV === "test"
        ? memory()
        : sqlite({ path: config.dataPath }),
    // Better Auth's session cookie decides who the user is.
    authenticate: async (request) => {
      const { auth } = await import("./auth");
      const session = await auth.api.getSession({ headers: request.headers });
      return session
        ? {
            userId: session.user.id,
            expiresAt: Math.floor(session.session.expiresAt.getTime() / 1000),
          }
        : null;
    },
  }).ready());
  // A failed start must not poison every later request.
  pending.catch(() => {
    if (instance === pending) instance = undefined;
  });
  return pending;
}
// #endregion bandchat-server-jazz

/**
 * Ends every sign-in of `userId` except the session `keepToken`: deletes the
 * user's other Better Auth sessions, then closes every open Jazz connection
 * of the user. The connection on `keepToken` signs in again on its cookie;
 * the others are signed out. A browser keeps its keys and its device record.
 *
 * Better Auth lists at most one page of sessions (100) at a time, so this
 * deletes page by page until a listing holds only `keepToken`. Returns how
 * many sessions it deleted.
 */
export async function endOtherSignIns(userId: string, keepToken: string): Promise<number> {
  const { auth } = await import("./auth");
  const sessions = (await auth.$context).internalAdapter;
  const deleted = new Set<string>();
  for (;;) {
    const others = (await sessions.listSessions(userId))
      .map((session) => session.token)
      .filter((token) => token !== keepToken);
    if (others.length === 0) break;
    // A session listed again after its delete would loop forever.
    if (others.some((token) => deleted.has(token)))
      throw new Error("Better Auth listed a session this route had already deleted.");
    await sessions.deleteSessions(others);
    for (const token of others) deleted.add(token);
  }
  // Without this, a connection opened on a deleted session stays signed in
  // until the expiry `authenticate` gave it.
  await (await jazz()).revokeUser(userId);
  return deleted.size;
}
