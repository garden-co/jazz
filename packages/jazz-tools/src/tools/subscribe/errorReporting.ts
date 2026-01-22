import type { JazzError } from "./JazzError";

/**
 * A platform agnostic way to check if we're in development mode
 *
 * Works in Node.js and bundled code, falls back to false if process is not available
 */
export const isDev = (function () {
  try {
    return process.env.NODE_ENV === "development";
  } catch {
    return false;
  }
})();

type CustomErrorReporterProps = {
  getPrettyStackTrace: () => string;
  jazzError: JazzError;
};

type CustomErrorReporter = (
  error: Error,
  props: CustomErrorReporterProps,
) => void;

let customErrorReporter: CustomErrorReporter | undefined;
let captureErrorCause: boolean = isDev;

/**
 * Turns on the additonal debug info coming from React hooks on the original subscription of the errors.
 *
 * Enabled by default in development mode.
 */
export function enableCaptureErrorCause(capture: boolean) {
  captureErrorCause = capture;
}

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
  return captureErrorCause ? new Error() : undefined;
}

export function captureError(error: Error, props: CustomErrorReporterProps) {
  if (customErrorReporter) {
    customErrorReporter(error, props);
  }
}
