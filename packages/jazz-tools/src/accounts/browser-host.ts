/**
 * @internal Whether this realm is a browser page or worker, not a server or
 * test runtime that happens to expose `localStorage` and `navigator.locks`
 * (as recent Node releases can). Only a browser retains the selected external
 * account across reloads.
 */
export function isBrowserHostRuntime(): boolean {
  if (typeof process !== "undefined" && process.versions?.node) return false;
  if (typeof navigator !== "undefined" && navigator.product === "ReactNative") return false;
  return (
    (typeof window !== "undefined" && typeof document !== "undefined") ||
    (typeof self !== "undefined" &&
      typeof (globalThis as { WorkerGlobalScope?: unknown }).WorkerGlobalScope !== "undefined")
  );
}
