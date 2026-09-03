import { resolve } from "node:path";

export interface ViteEnvConfig {
  envDir?: string | false;
  /** @deprecated Vite's programmatic API alias for disabling env files. */
  envFile?: false;
}

/**
 * Resolve the directory Vite would load env files from for this user config.
 *
 * Vite resolves a relative `envDir` from `root`, and its programmatic
 * `envFile: false` option wins over `envDir`. Keeping this here makes the
 * config hook (which sees user config) agree with configureServer (which sees
 * Vite's already-resolved `envDir`).
 */
export function resolveViteEnvDir(root: string, config: ViteEnvConfig): string | false {
  if (config.envFile === false || config.envDir === false) return false;
  return resolve(root, config.envDir ?? root);
}

/**
 * Load Vite's standard mode-aware env files from its resolved env directory
 * into process.env.
 *
 * Next.js does this itself before invoking next.config.ts, so its plugin sees
 * env vars through process.env for free. Vite and SvelteKit only expose
 * prefixed vars through import.meta.env and never populate process.env, so the
 * plugin has to backfill before reading server-side cloud-mode keys.
 *
 * Vite owns dotenv parsing, expansion, and the standard precedence order:
 * .env, .env.local, .env.[mode], then .env.[mode].local. Passing an empty
 * prefix asks it to return every key; only previously unset process values are
 * copied back, so shell and CI environment variables remain authoritative.
 */
export async function loadEnvFileIntoProcessEnv(
  envDir: string | false,
  mode: string,
): Promise<void> {
  // Vite is intentionally not a jazz-tools runtime dependency: resolve it
  // only when a Vite or SvelteKit integration actually loads framework env.
  const { loadEnv } = await import("vite");
  const loaded = loadEnv(mode, envDir, "");
  for (const [key, value] of Object.entries(loaded)) {
    if (process.env[key] === undefined) {
      process.env[key] = value;
    }
  }
}
