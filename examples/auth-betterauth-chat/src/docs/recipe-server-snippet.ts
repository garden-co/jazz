import { betterAuth } from "better-auth";
import { createJazz, sqlite } from "jazz-tools/backend";
import { app } from "../../schema";
import permissions from "../../jazz.permissions.json" with { type: "json" };
// #region betterauth-server
export const auth = betterAuth({
  // your database, email config, etc.
});

export const jazz = createJazz({
  app,
  permissions,
  store: sqlite({ path: "./data/jazz" }),
  // The Better Auth session cookie decides who the user is.
  authenticate: async (request) => {
    const session = await auth.api.getSession({ headers: request.headers });
    return session
      ? {
          userId: session.user.id,
          expiresAt: Math.floor(session.session.expiresAt.getTime() / 1000),
        }
      : null;
  },
});
// #endregion betterauth-server
