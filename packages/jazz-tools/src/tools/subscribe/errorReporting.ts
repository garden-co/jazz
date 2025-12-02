/**
 * Check if we're in development mode.
 * Stack traces are only captured in development to avoid overhead in production.
 */
export function isDev(): boolean {
  return process.env.NODE_ENV === "development";
}

/**
 * Capture a stack trace only in development mode.
 * Returns empty string in production to avoid overhead.
 */
export function captureStack(): string {
  return isDev() ? new Error().stack || "" : "";
}
