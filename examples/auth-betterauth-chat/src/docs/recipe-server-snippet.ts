import { betterAuth } from "better-auth";
import { createJazz, sqlite } from "jazz-tools/backend";
import { app } from "../../schema";
import permissions from "../../permissions";

// #region betterauth-server
export const auth = betterAuth({
  // your database, email config, etc.
});

export const jazz = await createJazz({
  appId: "my-app",
  app,
  permissions,
  store: sqlite({ path: "./data/jazz" }),
  // The Better Auth session cookie decides who the user is.
  authenticate: async (request) => {
    const session = await auth.api.getSession({ headers: request.headers });
    return session ? { userId: session.user.id } : null;
  },
});
// #endregion betterauth-server
