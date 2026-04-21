import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";

/**
 * Load .env from the given directory into process.env.
 *
 * Next.js does this itself before invoking next.config.ts, so its plugin
 * sees env vars through process.env for free. Vite and SvelteKit only load
 * Vite-prefixed vars via import.meta.env and never populate process.env —
 * so the plugin has to backfill before reading cloud-mode env keys.
 *
 * Only reads .env, not .env. Jazz writes a sibling .env file for
 * internal dev-server app-id persistence (see managed-runtime.ts), and we
 * don't want that leaking back through this loader.
 *
 * Does not overwrite values that are already in process.env; real shell
 * exports and CI env wins over .env.
 */
export function loadEnvFileIntoProcessEnv(root: string): void {
  const path = join(root, ".env");
  if (existsSync(path)) {
    const content = readFileSync(path, "utf8");
    for (let line of content.split("\n")) {
      if (line.endsWith("\r")) line = line.slice(0, -1);
      line = line.trim();
      if (!line || line.startsWith("#")) continue;
      const eq = line.indexOf("=");
      if (eq === -1) continue;
      const key = line.slice(0, eq).trim();
      let value = line.slice(eq + 1).trim();
      if (
        (value.startsWith('"') && value.endsWith('"')) ||
        (value.startsWith("'") && value.endsWith("'"))
      ) {
        value = value.slice(1, -1);
      }
      if (process.env[key] === undefined) {
        process.env[key] = value;
      }
    }
  }
}
