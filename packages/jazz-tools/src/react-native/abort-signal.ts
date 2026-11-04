/**
 * React Native's `AbortController` is the `abort-controller` polyfill, which
 * predates `abort(reason)`, `signal.reason` and `signal.throwIfAborted()`.
 * Jazz's write waits use all three, so on an engine whose `AbortSignal`
 * lacks them, add them as the DOM specifies. Where they exist, nothing
 * changes.
 */
export function completeAbortSignal(scope: typeof globalThis = globalThis): void {
  const Controller = scope.AbortController;
  const Signal = scope.AbortSignal;
  if (!Controller || !Signal) return;
  const signal = Signal.prototype as AbortSignal & { reason?: unknown };
  if (typeof signal.throwIfAborted === "function") return;

  const reasons = new WeakMap<AbortSignal, unknown>();
  const abort = Controller.prototype.abort;
  Object.defineProperty(Controller.prototype, "abort", {
    configurable: true,
    writable: true,
    value(this: AbortController, reason?: unknown) {
      if (!this.signal.aborted) {
        reasons.set(this.signal, reason === undefined ? abortError() : reason);
      }
      abort.call(this, reason);
    },
  });
  if (!("reason" in signal)) {
    Object.defineProperty(signal, "reason", {
      configurable: true,
      get(this: AbortSignal) {
        return reasons.get(this);
      },
    });
  }
  Object.defineProperty(signal, "throwIfAborted", {
    configurable: true,
    writable: true,
    value(this: AbortSignal) {
      if (this.aborted) throw this.reason;
    },
  });
}

function abortError(): Error {
  const error = new Error("This operation was aborted");
  error.name = "AbortError";
  return error;
}

completeAbortSignal();
