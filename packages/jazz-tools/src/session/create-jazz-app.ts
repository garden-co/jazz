import { createJazzSession, type JazzSessionConfig } from "./create-jazz-session.js";
import { createJazzAppOwner } from "./app.js";
export type JazzAppConfig = JazzSessionConfig;
/**
 * Create an observable application lifecycle immediately, including startup
 * failures and retry. Your backend's `authenticate` decides who is signed in;
 * call `reauthenticate()` after your own sign-in or sign-out.
 */
export function createJazzApp(config: JazzAppConfig, options: { start?: boolean } = {}) {
  return createJazzAppOwner(
    { ...config, initial: config.initial ?? "local-first" },
    createJazzSession,
    options,
  );
}
