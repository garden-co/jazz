/**
 * Options for the throttleTime function
 */
interface ThrottleOptions {
  /** Whether to invoke on the leading edge of the timeout (default: true) */
  leading?: boolean;
  /** Whether to invoke on the trailing edge of the timeout (default: true) */
  trailing?: boolean;
}

/**
 * Creates a throttled function that only invokes the provided function
 * at most once per every `wait` milliseconds.
 *
 * @param func - The function to throttle
 * @param wait - The number of milliseconds to throttle invocations to
 * @param options - The options object
 * @returns Returns the new throttled function
 */
export function throttleTime<T extends (...args: any[]) => any>(
  func: T,
  wait: number,
  options: ThrottleOptions = {},
): (...args: Parameters<T>) => ReturnType<T> | undefined {
  const { leading = true, trailing = true } = options;

  let timeout: NodeJS.Timeout | null = null;
  let previous = 0;
  let result: ReturnType<T> | undefined;
  let context: any;
  let args: Parameters<T>;

  const later = (): void => {
    previous = !leading ? 0 : Date.now();
    timeout = null;
    result = func.apply(context, args);
    if (!timeout) context = args = null!;
  };

  return function throttled(
    this: any,
    ...params: Parameters<T>
  ): ReturnType<T> | undefined {
    const now = Date.now();
    if (!previous && !leading) previous = now;

    const remaining = wait - (now - previous);

    context = this;
    args = params;

    if (remaining <= 0 || remaining > wait) {
      if (timeout) {
        clearTimeout(timeout);
        timeout = null;
      }

      previous = now;
      result = func.apply(context, args);

      if (!timeout) context = args = null!;
    } else if (!timeout && trailing) {
      timeout = setTimeout(later, remaining);
    }

    return result;
  };
}
