import type { JazzError } from "./JazzError";

type CustomErrorReporterProps = {
  getPrettyStackTrace: () => string;
  jazzError: JazzError;
};

type CustomErrorReporter = (
  error: Error,
  props: CustomErrorReporterProps,
) => void;

let customErrorReporter: CustomErrorReporter | undefined;

// A platform agnostic way to check if we're in development mode
// works in Node.js and bundled code, falls back to false if process is not available
const isDev = (function () {
  try {
    return process.env.NODE_ENV === "development";
  } catch {
    return false;
  }
})();

/**
 * Set a custom error reporter to be used instead of the default console.error.
 *
 * Useful for sending errors to a logging service or silence some annoying errors in production.
 */
export function setCustomErrorReporter(reporter?: CustomErrorReporter) {
  customErrorReporter = reporter;
}

/**
 * Check if we're in development mode.
 * Stack traces are only captured in development to avoid overhead in production.
 */
export function isCustomErrorReportingEnabled(): boolean {
  return customErrorReporter !== undefined;
}

/**
 * Capture a stack trace only in development mode.
 * Returns undefined in production to avoid overhead.
 */
export function captureStack() {
  return isDev || isCustomErrorReportingEnabled() ? new Error() : undefined;
}

export function captureError(error: Error, props: CustomErrorReporterProps) {
  if (customErrorReporter) {
    customErrorReporter(error, props);
  }
}
