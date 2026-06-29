// Auto-starts the inspector overlay for an app db. Shared by the
// react/svelte/vue/solid providers so the loader's module path lives in one
// place.
//
// The dynamic import("../dev/inspector-overlay/loader.js") is the lazy-chunk
// boundary that keeps the inspector (and dev-tools) out of the main bundle. The
// providers call this only inside a `process.env.NODE_ENV !== "production"`
// branch, so in production the call — and this whole module — is dropped, and
// the inspector never ships to prod.

// The overlay mounts only when the jazz dev plugin is active: the plugin exposes
// a public flag to the browser in dev (Vite-family on import.meta.env, Next on
// process.env), and we read both since each is undefined under the other's
// bundler (and `process` itself is undefined in a Vite browser bundle, hence the
// guard). Without the plugin there's no flag and no toggle — correct, since
// nothing would serve the iframe. Read process.env.NEXT_PUBLIC_JAZZ_INSPECTOR as
// a literal: Next only static-inlines that exact member access, not an alias.
function jazzDevPluginActive(): boolean {
  const viteEnv = (import.meta as unknown as { env?: Record<string, unknown> }).env;
  const nextFlag =
    typeof process !== "undefined" ? process.env.NEXT_PUBLIC_JAZZ_INSPECTOR : undefined;
  return Boolean(viteEnv?.VITE_JAZZ_INSPECTOR || nextFlag);
}

/**
 * Mount the inspector overlay + attach the dev-tools bridge for a db. The
 * providers only call this in dev; it additionally no-ops unless the jazz dev
 * plugin is serving the inspector. Safe to call on every provider effect run
 * with no guard here: the overlay UI mounts once globally and the bridge
 * registers once per db (see attachDevTools), so repeat calls are idempotent.
 */
export function startInspectorOnce(db: object): void {
  if (!jazzDevPluginActive()) return;
  void import("../dev/inspector-overlay/loader.js").then(({ startInspectorOverlay }) =>
    startInspectorOverlay(db),
  );
}
