/** Preserve the operation that failed while still completing both teardown
 * steps. A teardown-only failure remains observable to the acceptance host. */
export async function finishSeedClient(
  unsubscribe: () => void,
  shutdown: () => Promise<void>,
  preservePrimaryFailure: boolean,
): Promise<void> {
  let teardownError: unknown;
  try {
    unsubscribe();
  } catch (error) {
    teardownError = error;
  }
  try {
    await shutdown();
  } catch (error) {
    teardownError ??= error;
  }
  if (!preservePrimaryFailure && teardownError !== undefined) throw teardownError;
}
