/**
 * Suppresses the inert WASM "memory access out of bounds" trap that fires when a
 * tab with multiple Jazz runtimes navigates away mid-sync — but only inside the
 * `pagehide` teardown window, so a genuine OOB during normal operation still
 * surfaces. See `specs/todo/issues/wasm-memory-access-oob-multi-client-teardown.md`.
 */

/** Matches the wasm trap signatures a corrupted-on-teardown heap produces. */
export function isWasmTeardownTrap(message: string | undefined | null): boolean {
  if (!message) return false;
  return /memory access out of bounds|out of bounds memory access|table index is out of bounds|null function or function signature mismatch|unreachable executed|RuntimeError: unreachable/i.test(
    message,
  );
}

let teardownInProgress = false;
let suppressorInstalled = false;

/**
 * Open the teardown window: from now until the page is gone (or restored from
 * the back/forward cache) the inert WASM-teardown trap is suppressed.
 * Idempotent; safe to call repeatedly.
 */
export function markWasmTeardownInProgress(): void {
  installWasmTeardownTrapSuppressor();
  teardownInProgress = true;
}

/**
 * Install the window error/rejection handlers that swallow the teardown trap.
 * Idempotent; no-op outside a browser. Call early so they're armed before the
 * first `pagehide`.
 */
export function installWasmTeardownTrapSuppressor(): void {
  if (suppressorInstalled) return;
  if (typeof window === "undefined") return;
  suppressorInstalled = true;

  window.addEventListener(
    "error",
    (event: ErrorEvent) => {
      if (!teardownInProgress) return;
      const message = event.message || event.error?.message;
      if (!isWasmTeardownTrap(message)) return;
      // preventDefault stops the browser logging the inert trap; the capture
      // phase + stopImmediatePropagation keeps it from the app's own handlers
      // when this listener runs first.
      event.preventDefault();
      event.stopImmediatePropagation();
    },
    true,
  );

  window.addEventListener(
    "unhandledrejection",
    (event: PromiseRejectionEvent) => {
      if (!teardownInProgress) return;
      const reason = event.reason as Error | string | undefined;
      const message = typeof reason === "string" ? reason : reason?.message;
      if (!isWasmTeardownTrap(message)) return;
      event.preventDefault();
    },
    true,
  );

  // A page restored from the back/forward cache is alive again; reopen the
  // suppression window only on the next navigation.
  window.addEventListener("pageshow", () => {
    teardownInProgress = false;
  });
}

export function isWasmTeardownInProgress(): boolean {
  return teardownInProgress;
}

/**
 * Test seam: reset module state between tests.
 * @internal
 */
export function resetWasmTeardownTrapSuppressorForTest(): void {
  teardownInProgress = false;
  suppressorInstalled = false;
}
