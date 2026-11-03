import { createJazzSession, type JazzSessionConfig } from "./create-jazz-session.js";
import { authForUser, createJazzAppOwner, type JazzAuth } from "./app.js";
export type JazzAppConfig = JazzSessionConfig & {
  auth?: JazzAuth;
  /**
   * Who your auth library says is signed in: `undefined` while it loads,
   * `null` when signed out, and the user's stable id when signed in. When it
   * changes, Jazz signs in again through your backend, whose `authenticate`
   * decides who the user is; `null` closes the account's client.
   */
  user?: string | null;
};
/** Create an observable application lifecycle immediately, including startup failures and retry. */
export function createJazzApp(config: JazzAppConfig, options: { start?: boolean } = {}) {
  const { user, auth: explicitAuth, ...rest } = config;
  const auth = authForUser("user" in config, user, explicitAuth);
  return createJazzAppOwner(
    { ...rest, auth, initial: rest.initial ?? (auth ? undefined : "local-first") },
    createJazzSession,
    options,
  );
}
