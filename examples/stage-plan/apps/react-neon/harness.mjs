// Comparison harness hooks for the Neon Realtime StagePlan (see ../../../../dev/app-comparisons/SPEC.md):
// hosted Neon Postgres with Realtime (aws-us-east-1, branch "stage-plan"), and
// the app's Hono server locally (anonymous sign-in, sealing, writes, built app).
import { readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { ensureNeon } from "../../../../dev/app-comparisons/harness/adapters/neon.mjs";
import { serve, sh } from "../../../../dev/app-comparisons/harness/adapters/vite-app.mjs";

const dir = dirname(fileURLToPath(import.meta.url));
const PORT = 4320;
const ORIGIN = `http://127.0.0.1:${PORT}`;
const neon = () => ensureNeon(dir, { branch: "stage-plan" });
export const distDir = "dist";

export async function seed() {
  const out = sh(dir, "pnpm exec tsx scripts/seed.ts", neon());
  const ctx = JSON.parse(out.trim().split("\n").at(-1));
  writeFileSync(`${dir}/.bench-ctx.json`, JSON.stringify(ctx));
  return ctx;
}

/** The ctx of the last seed(), for --no-seed runs. */
export const lastSeed = () => JSON.parse(readFileSync(`${dir}/.bench-ctx.json`, "utf8"));

export async function start() {
  const env = neon();
  sh(dir, "pnpm exec vite build --logLevel error", { ...env, VITE_NEON_REALTIME_URL: env.NEON_REALTIME_URL });
  const stop = await serve(dir, "pnpm exec tsx server/index.ts", `${ORIGIN}/api/login`, {
    ...env,
    AUTH_SECRET: "stage-plan-bench-secret",
    NODE_ENV: "production",
    PORT: String(PORT),
    SERVE_DIST: "dist",
  });
  return {
    url: ORIGIN,
    // The Realtime endpoint answers plain HTTP slowly (it's a WebSocket API), so
    // rtt is a SELECT 1 on the Neon database through the local app server.
    rttUrl: `${ORIGIN}/api/health`,
    where: "hosted (Neon Postgres + Realtime, aws-us-east-1); Hono app server local",
    stop,
  };
}
