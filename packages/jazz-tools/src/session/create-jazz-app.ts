import { createJazzSession, type JazzSessionConfig } from "./create-jazz-session.js";
import { createJazzAppOwner, type JazzAuth } from "./app.js";
export type JazzAppConfig = JazzSessionConfig & { auth?: JazzAuth };
/** Create an observable application lifecycle immediately, including startup failures and retry. */
export function createJazzApp(config: JazzAppConfig) {
  return createJazzAppOwner(
    { ...config, initial: config.initial ?? (config.auth ? undefined : "local-first") },
    createJazzSession,
  );
}
