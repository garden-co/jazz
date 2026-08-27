type CleanupStep = () => void | PromiseLike<void>;

/** Runs teardown steps in order, then rethrows the first value that failed. */
export async function runCleanupSteps(steps: readonly CleanupStep[]): Promise<void> {
  let failed = false;
  let firstError: unknown;

  for (const step of steps) {
    try {
      await step();
    } catch (error) {
      if (!failed) {
        failed = true;
        firstError = error;
      }
    }
  }

  if (failed) throw firstError;
}
