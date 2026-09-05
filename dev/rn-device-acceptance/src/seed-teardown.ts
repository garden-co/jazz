export type SeedBoundary =
  | "js-before-core-await"
  | "js-core-await-returned"
  | "js-before-unsubscribe"
  | "js-after-unsubscribe"
  | "js-before-shutdown"
  | "js-after-shutdown";

/** Preserve the operation that failed while still completing both teardown
 * steps. A teardown-only failure remains observable to the acceptance host. */
export async function finishSeedClient(
  unsubscribe: () => void,
  shutdown: () => Promise<void>,
  preservePrimaryFailure: boolean,
  boundary?: (code: SeedBoundary) => void,
): Promise<void> {
  let teardownError: unknown;
  try {
    boundary?.("js-before-unsubscribe");
    unsubscribe();
    boundary?.("js-after-unsubscribe");
  } catch (error) {
    teardownError = error;
  }
  try {
    boundary?.("js-before-shutdown");
    await shutdown();
    boundary?.("js-after-shutdown");
  } catch (error) {
    teardownError ??= error;
  }
  if (!preservePrimaryFailure && teardownError !== undefined) throw teardownError;
}
