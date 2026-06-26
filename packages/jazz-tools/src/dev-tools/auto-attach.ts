// Auto-starts the inspector overlay for an app db, once per db. Shared by the
// react/svelte/vue/solid providers so the once-per-db guard and the loader's
// module path live in a single place.
//
// The dynamic import("../dev/inspector-overlay/loader.js") is the lazy-chunk
// boundary that keeps the inspector (and dev-tools) out of the main bundle. The
// providers call startInspectorOnce only inside a `process.env.NODE_ENV !==
// "production"` branch, so in production the call — and this whole module — is
// dropped, and the inspector never ships to prod.
const attachedDbs = new WeakSet<object>();

/** Mount the inspector + attach the bridge for a db, at most once per db. */
export function startInspectorOnce(db: object): void {
  if (attachedDbs.has(db)) return;
  attachedDbs.add(db);
  void import("../dev/inspector-overlay/loader.js").then(({ startInspectorOverlay }) =>
    startInspectorOverlay(db),
  );
}
