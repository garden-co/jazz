import { describe, expect, it } from "vitest";
import { completeAbortSignal } from "./abort-signal.js";

/** The shape of React Native's `abort-controller` polyfill: no reason, no throwIfAborted. */
function polyfillScope() {
  class AbortSignal {
    aborted = false;
    readonly listeners: Array<() => void> = [];
    addEventListener(_type: "abort", listener: () => void) {
      this.listeners.push(listener);
    }
  }
  class AbortController {
    readonly signal = new AbortSignal();
    abort() {
      if (this.signal.aborted) return;
      this.signal.aborted = true;
      for (const listener of this.signal.listeners) listener();
    }
  }
  return { AbortController, AbortSignal } as unknown as typeof globalThis;
}

describe("completeAbortSignal", () => {
  it("gives React Native's polyfill abort reasons and throwIfAborted", () => {
    const scope = polyfillScope();
    completeAbortSignal(scope);
    const control = new scope.AbortController();
    expect(control.signal.reason).toBeUndefined();
    expect(() => control.signal.throwIfAborted()).not.toThrow();

    const seen: unknown[] = [];
    control.signal.addEventListener("abort", () => seen.push(control.signal.reason));
    const reason = new Error("timed out");
    control.abort(reason);
    // The reason is set before listeners run, and a second abort keeps it.
    expect(seen).toEqual([reason]);
    control.abort(new Error("later"));
    expect(control.signal.reason).toBe(reason);
    expect(() => control.signal.throwIfAborted()).toThrow(reason);

    const plain = new scope.AbortController();
    plain.abort();
    expect(plain.signal.reason).toMatchObject({ name: "AbortError" });
    expect(plain.signal.reason).toBe(plain.signal.reason);
  });

  it("leaves a complete AbortSignal alone", () => {
    const abort = AbortController.prototype.abort;
    completeAbortSignal(globalThis);
    expect(AbortController.prototype.abort).toBe(abort);
  });
});
