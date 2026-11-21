// Comparison harness hooks for the Zero Wequencer (see ../../../../dev/app-comparisons/SPEC.md): local
// Postgres (wal_level=logical, see NOTES.md), local zero-cache, and the Next
// app (Better Auth, Zero's /api/zero endpoints).
import { readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { ensureInstalled, ensurePostgres, PG_URL, serve, sh, startZeroCache } from "../../../../dev/app-comparisons/harness/adapters/zero-local.mjs";

const dir = dirname(fileURLToPath(import.meta.url));
const PORT = 4330;
const ZERO_PORT = 4868;
const ORIGIN = `http://127.0.0.1:${PORT}`;
const DB = "wequencer_zero";
const ENV = {
  NEXT_PUBLIC_APP_ORIGIN: ORIGIN,
  NEXT_PUBLIC_ZERO_CACHE_PORT: String(ZERO_PORT),
  BETTER_AUTH_SECRET: "bench-only-secret-not-for-deployments-0003",
  ZERO_UPSTREAM_DB: PG_URL(DB),
  ZERO_REPLICA_FILE: ".zero/replica.db",
};
export const distDir = ".next/static";

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
  sh(dir, "pnpm exec next build > /tmp/wequencer-zero-build.log", { ...ENV, NODE_ENV: "production" });
  const stopServer = await serve(dir, `pnpm exec next start -H 127.0.0.1 -p ${PORT}`, `${ORIGIN}/`, {
    ...ENV,
    NODE_ENV: "production",
  });
  const zero = await startZeroCache(dir, { port: ZERO_PORT, db: DB, origin: ORIGIN });
  return {
    url: ORIGIN,
    rttUrl: zero.url,
    where: "local (Postgres 18 + zero-cache + Next/Better Auth server, same machine)",
    stop: async () => {
      await zero.stop();
      await stopServer();
    },
  };
}

/** Signs `page`'s context in as fixture user `key` through Better Auth's API. */
export async function signIn(page, key, ctx) {
  const user = ctx.users[key];
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
