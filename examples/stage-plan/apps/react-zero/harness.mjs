// Comparison harness hooks for the Zero StagePlan (see ../../../../dev/app-comparisons/SPEC.md):
// local Postgres (wal_level=logical, see NOTES.md), local zero-cache, and the
// app's server (anonymous sign-in, Zero's query/mutate endpoints, the built app).
import { readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { ensureInstalled, ensurePostgres, PG_URL, serve, sh, startZeroCache } from "../../../../dev/app-comparisons/harness/adapters/zero-local.mjs";

const dir = dirname(fileURLToPath(import.meta.url));
const PORT = 4310;
const ZERO_PORT = 4848;
const ORIGIN = `http://127.0.0.1:${PORT}`;
const DB = "stage_plan_zero";
const ENV = { ZERO_UPSTREAM_DB: PG_URL(DB), ZERO_REPLICA_FILE: ".zero/replica.db", AUTH_SECRET: "stage-plan-bench-secret" };
export const distDir = "dist";

export async function seed() {
  ensureInstalled(dir);
  ensurePostgres(dir);
  const out = sh(dir, "pnpm exec tsx scripts/seed.ts", ENV);
  const ctx = JSON.parse(out.trim().split("\n").at(-1));
  writeFileSync(`${dir}/.bench-ctx.json`, JSON.stringify(ctx));
  return ctx;
}

/** The ctx of the last seed(), for --no-seed runs. */
export const lastSeed = () => JSON.parse(readFileSync(`${dir}/.bench-ctx.json`, "utf8"));

export async function start() {
  ensureInstalled(dir);
  ensurePostgres(dir);
  sh(dir, "pnpm exec vite build --logLevel error", { VITE_ZERO_CACHE_PORT: String(ZERO_PORT) });
  const stopServer = await serve(dir, "pnpm exec tsx server/index.ts", `${ORIGIN}/api/login`, {
    ...ENV,
    NODE_ENV: "production",
    PORT: String(PORT),
    SERVE_DIST: "dist",
  });
  const zero = await startZeroCache(dir, { port: ZERO_PORT, db: DB, origin: ORIGIN });
  return {
    url: ORIGIN,
    rttUrl: zero.url,
    where: "local (Postgres 18 + zero-cache + Hono app server, same machine)",
    stop: async () => {
      await zero.stop();
      await stopServer();
    },
  };
}
