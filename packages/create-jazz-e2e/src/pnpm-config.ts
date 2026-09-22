/**
 * Pinned upstream versions the harness needs to override regardless of what
 * any individual starter's transitive resolution lands on.
 *
 * `kysely`: `@better-auth/kysely-adapter@1.6.12` declares
 * `peerDependencies.kysely: "^0.28.17 || ^0.29.0"` but still imports
 * `DEFAULT_MIGRATION_LOCK_TABLE` and `DEFAULT_MIGRATION_TABLE` from kysely —
 * symbols kysely removed in 0.29.0 (a legitimate breaking change under
 * pre-1.0 semver). The widened peer range opts the adapter into a kysely it
 * doesn't actually work with, and the SSR-bundled starters (next-*,
 * sveltekit-*) crash trying to resolve the missing exports. Pinning to the
 * last 0.28.x keeps the e2e build green until upstream catches up.
 */
const UPSTREAM_PINS: Record<string, string> = {
  kysely: "0.28.17",
};

/**
 * Render the pnpm workspace override config consumed by each scaffolded app.
 */
export function renderScaffoldedPnpmConfig(tarballs: Record<string, string>): string {
  const overrideLines = [
    ...Object.entries(tarballs).map(([pkg, tgz]) => `  "${pkg}": "file:${tgz}"`),
    ...Object.entries(UPSTREAM_PINS).map(([pkg, v]) => `  "${pkg}": "${v}"`),
  ].join("\n");
  return `overrides:\n${overrideLines}\n`;
}
