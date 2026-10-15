// Diagnostic switches; in the browser there is no process environment, so
// they read a global override once and cache it.
const cache = new Map<string, boolean>();
function cached(name: string): boolean {
  let v = cache.get(name);
  if (v === undefined) {
    v = !!(globalThis as any).__JAZZ_ENV__?.[name];
    cache.set(name, v);
  }
  return v;
}
export const coveredInputTrace = () => cached("JAZZ_COVERED_INPUT_TRACE");
export const rehydrateTrace = () => cached("JAZZ_REHYDRATE_TRACE");
export const forceSingletonVersionCarriers = () => cached("JAZZ_FORCE_SINGLETON_VERSION_CARRIERS");
