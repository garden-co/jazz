/**
 * Metro neither follows `new URL(..., import.meta.url)` nor keeps a
 * `new SharedWorker(new URL(...))` a SharedWorker, so Jazz's browser assets
 * need `withJazz` there. Without it the WASM request is a 404.
 */
export const METRO_SETUP_ERROR =
  "Jazz's local-first client needs its Metro setup on Expo web: wrap your Metro config in withJazz from jazz-tools/expo/metro (module.exports = withJazz(getDefaultConfig(__dirname))) and restart Metro with --clear.";

/**
 * Jazz names its worker chunk with Expo's undocumented
 * `require.unstable_resolveWorker`. A Metro without it can't build the
 * worker, whatever the app's config.
 */
export const METRO_WORKER_API_ERROR =
  "Jazz's local-first client on Expo web needs Expo's Metro to build its worker (require.unstable_resolveWorker), and this bundle's Metro did not: bundle with getDefaultConfig from expo/metro-config wrapped in withJazz from jazz-tools/expo/metro (Expo SDK 54 and later), restart Metro with --clear, or use the thin client.";

/**
 * Why Metro's broker worker chunk did not start: the app's Metro config, or a
 * stale Metro cache that still serves the chunk from before it.
 */
export const METRO_WORKER_START_HINT =
  " Under Metro (Expo web), Jazz's worker is the Expo worker chunk withJazz from jazz-tools/expo/metro builds: check that metro.config.js wraps getDefaultConfig from expo/metro-config in withJazz, restart Metro with --clear, and leave runtimeSources.brokerWorkerUrl unset (a URL set there is started as a module script, not as Metro's chunk).";

/** Whether this code runs in a Metro bundle. Metro declares this global in every bundle and worker chunk. */
export function isMetroBundle(): boolean {
  return "__METRO_GLOBAL_PREFIX__" in globalThis;
}
