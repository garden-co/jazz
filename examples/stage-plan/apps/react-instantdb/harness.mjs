// Comparison harness hooks for the InstantDB StagePlan (see ../../../../dev/app-comparisons/SPEC.md).
// The backend is a hosted, temporary Instant app (no account): created on
// first use into .env, with instant.schema.ts and instant.perms.ts pushed on
// every seed.
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { serve, sh } from "../../../../dev/app-comparisons/harness/adapters/vite-app.mjs";

const dir = dirname(fileURLToPath(import.meta.url));
const PORT = 4411;
export const distDir = "dist";

/**
 * Installs dependencies on a fresh checkout and returns the app id and admin
 * token in .env, creating a temporary Instant app if there is none.
 */
export function ensureApp() {
  if (!existsSync(`${dir}/node_modules`)) sh(dir, "pnpm install --frozen-lockfile >&2");
  const file = `${dir}/.env`;
  if (!existsSync(file)) {
    const out = sh(dir, "pnpm exec instant-cli init-without-files --title stage-plan-bench --temp");
    const { app } = JSON.parse(out.slice(out.indexOf("{")));
    writeFileSync(
      file,
      `VITE_INSTANT_APP_ID=${app.appId}\nINSTANT_APP_ADMIN_TOKEN=${app.adminToken}\n`,
    );
  }
  return Object.fromEntries(
    readFileSync(file, "utf8")
      .trim()
      .split("\n")
      .map((line) => line.split("=", 2)),
  );
}

export async function seed() {
  const env = ensureApp();
  sh(
    dir,
    `pnpm exec instant-cli push all --yes --app "$VITE_INSTANT_APP_ID" --token "$INSTANT_APP_ADMIN_TOKEN" >&2`,
    env,
  );
  const out = sh(dir, "pnpm exec tsx seed.ts", env);
  return JSON.parse(out.trim().split("\n").at(-1));
}

export async function start() {
  const env = ensureApp();
  sh(dir, "pnpm exec vite build --logLevel error", env);
  const url = `http://127.0.0.1:${PORT}`;
  const stop = await serve(
    dir,
    `pnpm exec vite preview --host 127.0.0.1 --port ${PORT} --strictPort`,
    url,
  );
  return {
    url,
    stop,
    where: "hosted (Instant Cloud temporary app, api.instantdb.com via CloudFront IAD / us-east)",
    rttUrl: "https://api.instantdb.com/",
  };
}
