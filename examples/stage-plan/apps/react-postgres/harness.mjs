// Comparison harness hooks for the plain-Postgres StagePlan (see ../../../../dev/app-comparisons/SPEC.md):
// local Postgres 18 (see NOTES.md) and the app's Node server (Hono + pg + ws),
// which serves the built app, the REST/RPC endpoints and the live WebSocket.
import { execSync } from "node:child_process";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { serve, sh } from "../../../../dev/app-comparisons/harness/adapters/vite-app.mjs";

const dir = dirname(fileURLToPath(import.meta.url));
const PORT = 4320;
const ORIGIN = `http://127.0.0.1:${PORT}`;
const ENV = {
  DATABASE_URL: "postgres://postgres:pass@127.0.0.1:5432/stage_plan_postgres",
  AUTH_SECRET: "stage-plan-bench-secret",
};
export const distDir = "dist";

function prepare() {
  if (!existsSync(`${dir}/node_modules/.bin/tsx`)) {
    execSync("pnpm install --frozen-lockfile", { cwd: dir, stdio: ["ignore", "inherit", "inherit"] });
  }
  try {
    execSync("pg_isready -h 127.0.0.1 -q");
  } catch {
    execSync("sudo service postgresql start", { stdio: "inherit" });
  }
}

export async function seed() {
  prepare();
  const out = sh(dir, "pnpm exec tsx scripts/seed.ts", ENV);
  const ctx = JSON.parse(out.trim().split("\n").at(-1));
  writeFileSync(`${dir}/.bench-ctx.json`, JSON.stringify(ctx));
  return ctx;
}

/** The ctx of the last seed(), for --no-seed runs. */
export const lastSeed = () => JSON.parse(readFileSync(`${dir}/.bench-ctx.json`, "utf8"));

export async function start() {
  prepare();
  sh(dir, "pnpm exec vite build --logLevel error");
  const stop = await serve(dir, "pnpm exec tsx server/index.ts", `${ORIGIN}/api/health`, {
    ...ENV,
    NODE_ENV: "production",
    PORT: String(PORT),
    SERVE_DIST: "dist",
  });
  return {
    url: ORIGIN,
    rttUrl: `${ORIGIN}/api/health`,
    where: "local (Postgres 18 + Hono/pg/ws app server, same machine)",
    stop,
  };
}
