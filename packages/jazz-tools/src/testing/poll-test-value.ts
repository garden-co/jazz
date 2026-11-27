/** Polls until `read` returns a value other than `undefined`. */
export async function pollTestValue<T>(
  read: () => Promise<T | undefined>,
  what: string | (() => string),
  timeoutMs = 20_000,
  { intervalMs = 100, retryErrors = true } = {},
): Promise<T> {
  const end = Date.now() + timeoutMs;
  for (;;) {
    let failure: unknown;
    const value = await read().catch((error) => {
      if (!retryErrors) throw error;
      failure = error;
      return undefined;
    });
    if (value !== undefined) return value;
    if (Date.now() > end) {
      const message =
        typeof what === "function" ? what() : `timed out waiting for ${what}: ${String(failure)}`;
      throw new Error(message);
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
}
