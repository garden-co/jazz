/** One deadline covers both response headers and body consumption. */
export async function withAuthRequestDeadline<T>(
  operation: (signal: AbortSignal) => Promise<T>,
  timeoutError: Error,
): Promise<T> {
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => {
          // Settle with the deadline error before aborting: an abort-aware fetch
          // must not turn a timeout into a caller-cancellation or parsing error.
          reject(timeoutError);
          controller.abort(timeoutError);
        }, 30_000);
      }),
      // The race also bounds injected fetchers that do not honor AbortSignal.
      operation(controller.signal),
    ]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}
