/**
 * Metro neither follows `new URL(..., import.meta.url)` nor keeps a
 * `new SharedWorker(new URL(...))` a SharedWorker, so Jazz's browser assets
 * need `withJazz` there. Without it the WASM request is a 404.
 */
export const METRO_SETUP_ERROR =
  "Jazz's local-first client needs its Metro setup on Expo web: wrap your Metro config in withJazz from jazz-tools/expo/metro (module.exports = withJazz(getDefaultConfig(__dirname))) and restart Metro with --clear.";

/** Whether this code runs in a Metro bundle. Metro declares this global in every bundle and worker chunk. */
export function isMetroBundle(): boolean {
  return "__METRO_GLOBAL_PREFIX__" in globalThis;
}
