// Comparison harness hooks for the Zero BandChat (see ../../../../dev/app-comparisons/SPEC.md): local
// Postgres (wal_level=logical), local zero-cache, and the app's Hono server
// (Better Auth, Zero's query/mutate endpoints, the built SPA).
import { readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { ensureInstalled, ensurePostgres, PG_URL, serve, sh, startZeroCache } from "../../../../dev/app-comparisons/harness/adapters/zero-local.mjs";

const dir = dirname(fileURLToPath(import.meta.url));
const PORT = 4320;
const ZERO_PORT = 4858;
const ORIGIN = `http://127.0.0.1:${PORT}`;
const DB = "band_chat_zero";
const ENV = {
  APP_ORIGIN: ORIGIN,
  BETTER_AUTH_SECRET: "bench-only-secret-not-for-deployments-0001",
  ZERO_UPSTREAM_DB: PG_URL(DB),
  ZERO_REPLICA_FILE: ".zero/replica.db",
};
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
  const stopServer = await serve(dir, "pnpm exec tsx server/index.ts", `${ORIGIN}/health`, {
    ...ENV,
    NODE_ENV: "production",
    PORT: String(PORT),
  });
  const zero = await startZeroCache(dir, { port: ZERO_PORT, db: DB, origin: ORIGIN });
  return {
    url: ORIGIN,
    rttUrl: zero.url,
    where: "local (Postgres 18 + zero-cache + Hono/Better Auth server, same machine)",
    stop: async () => {
      await zero.stop();
      await stopServer();
    },
  };
}

/** Signs `page`'s context in as fixture user `key` through Better Auth's API. */
export async function signIn(page, key, ctx) {
  const user = ctx.users[key];
  // Untimed. Better Auth rate-limits sign-ins in production: wait it out.
  let res;
  for (let attempt = 0; attempt < 30; attempt++) {
    res = await page.context().request.post(`${ORIGIN}/api/auth/sign-in/email`, {
      data: { email: user.email, password: user.password },
      headers: { origin: ORIGIN },
    });
    if (res.status() !== 429) break;
    await new Promise((r) => setTimeout(r, 1000));
  }
  if (!res.ok()) throw new Error(`sign-in failed: ${res.status()} ${await res.text()}`);
}
