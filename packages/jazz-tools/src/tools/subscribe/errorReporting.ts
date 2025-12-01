import type { CoValue, ID } from "../internal.js";
import { CoValueLoadingState } from "./types.js";

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

export type JazzErrorEvent = {
  type: "unavailable" | "unauthorized" | "validation";
  coValueId: ID<CoValue> | undefined;
  message: string;
  stack: string;
  timestamp: number;
  errorObject: any;
};

type ErrorListener = (event: JazzErrorEvent) => void;

class JazzErrorReporter {
  private listeners: Set<ErrorListener> = new Set();
  private errorHistory: JazzErrorEvent[] = [];
  private maxHistorySize = 100;

  /**
   * Subscribe to all Jazz errors for debugging purposes.
   * Returns an unsubscribe function.
   *
   * Example usage in React Native:
   * ```typescript
   * import { jazzErrorReporter } from 'jazz-tools';
   *
   * // In your app initialization
   * jazzErrorReporter.onError((event) => {
   *   console.error('Jazz Error:', event);
   *   // Or show a custom UI notification
   *   Alert.alert('Jazz Error', event.message);
   * });
   * ```
   */
  onError(listener: ErrorListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  /**
   * Report an error to all listeners
   * @internal
   */
  reportError(event: JazzErrorEvent): void {
    // Add to history
    this.errorHistory.unshift(event);
    if (this.errorHistory.length > this.maxHistorySize) {
      this.errorHistory.pop();
    }

    // Notify all listeners
    this.listeners.forEach((listener) => {
      try {
        listener(event);
      } catch (e) {
        // Don't let listener errors break the error reporting
        console.error("Error in Jazz error listener:", e);
      }
    });
  }

  /**
   * Get the recent error history (up to 100 errors)
   */
  getErrorHistory(): ReadonlyArray<JazzErrorEvent> {
    return this.errorHistory;
  }

  /**
   * Clear error history
   */
  clearHistory(): void {
    this.errorHistory = [];
  }

  /**
   * Get current listener count
   */
  getListenerCount(): number {
    return this.listeners.size;
  }
}

/**
 * Global Jazz error reporter instance.
 *
 * Use this to subscribe to all Jazz errors in your application:
 *
 * ```typescript
 * import { jazzErrorReporter } from 'jazz-tools';
 *
 * jazzErrorReporter.onError((error) => {
 *   console.log('Jazz error occurred:', error);
 * });
 * ```
 */
export const jazzErrorReporter = new JazzErrorReporter();

/**
 * Helper to create and report a Jazz error event
 * @internal
 */
export function reportJazzError(
  type: JazzErrorEvent["type"],
  coValueId: ID<CoValue> | undefined,
  message: string,
  stack: string,
  errorObject: any,
): void {
  jazzErrorReporter.reportError({
    type,
    coValueId,
    message,
    stack,
    timestamp: Date.now(),
    errorObject,
  });
}
