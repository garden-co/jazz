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

// The inspector overlay is experimental: it only mounts when a dev plugin opts
// in via `experimental_inspector`, which injects a public env flag the host
// bundler inlines. We read both channels because this shared module is bundled
// by whichever framework the app uses, and the overlay is dev-only — so this
// runs against a live env object (Vite) or an inlined value (Next), never in a
// shipped production bundle.
//   Vite-based (React/Vue/Solid/SvelteKit): import.meta.env.VITE_JAZZ_INSPECTOR
//   Next:                                   process.env.NEXT_PUBLIC_JAZZ_INSPECTOR
function inspectorEnabled(): boolean {
  let viteFlag: unknown;
  try {
    viteFlag = (import.meta as unknown as { env?: Record<string, unknown> }).env
      ?.VITE_JAZZ_INSPECTOR;
  } catch {
    /* import.meta.env is unavailable in non-Vite bundlers */
  }
  const nextFlag =
    typeof process !== "undefined" ? process.env?.NEXT_PUBLIC_JAZZ_INSPECTOR : undefined;
  return Boolean(viteFlag ?? nextFlag);
}

/**
 * Mount the inspector + attach the bridge for a db, at most once per db. No-op
 * unless a dev plugin enabled the experimental inspector overlay.
 */
export function startInspectorOnce(db: object): void {
  if (!inspectorEnabled()) return;
  if (attachedDbs.has(db)) return;
  attachedDbs.add(db);
  void import("../dev/inspector-overlay/loader.js").then(({ startInspectorOverlay }) =>
    startInspectorOverlay(db),
  );
}
