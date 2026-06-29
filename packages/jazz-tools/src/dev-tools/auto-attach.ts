// Auto-starts the inspector overlay for an app db. Shared by the
// react/svelte/vue/solid providers so the loader's module path lives in one
// place.
//
// The dynamic import("../dev/inspector-overlay/loader.js") is the lazy-chunk
// boundary that keeps the inspector (and dev-tools) out of the main bundle. The
// providers call this only inside a `process.env.NODE_ENV !== "production"`
// branch, so in production the call — and this whole module — is dropped, and
// the inspector never ships to prod.

// The inspector overlay is experimental and dev-only: it mounts only when a dev
// plugin sets the public flag below. Vite-family bundlers inline it on
// import.meta.env, Next on process.env — check both, since each is undefined
// under the other's bundler (and `process` itself is undefined in a Vite
// browser bundle, hence the guard).
function inspectorEnabled(): boolean {
  const viteEnv = (import.meta as unknown as { env?: Record<string, unknown> }).env;
  const nextEnv = typeof process !== "undefined" ? process.env : undefined;
  return Boolean(viteEnv?.VITE_JAZZ_INSPECTOR || nextEnv?.NEXT_PUBLIC_JAZZ_INSPECTOR);
}

/**
 * Mount the inspector overlay + attach the dev-tools bridge for a db. No-op
 * unless a dev plugin enabled the experimental inspector. Safe to call on every
 * provider effect run with no guard here: the overlay UI mounts once globally
 * and the bridge registers once per db (see attachDevTools), so repeat calls are
 * idempotent.
 */
export function startInspectorOnce(db: object): void {
  if (!inspectorEnabled()) return;
  void import("../dev/inspector-overlay/loader.js").then(({ startInspectorOverlay }) =>
    startInspectorOverlay(db),
  );
}
