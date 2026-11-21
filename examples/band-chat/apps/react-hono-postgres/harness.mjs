// Comparison harness hooks for the plain-Postgres BandChat (see ../../../../dev/app-comparisons/SPEC.md):
// local Postgres 18 and the app's Hono server (Better Auth, REST/RPC, the live
// WebSocket, the built SPA).
import { execSync } from "node:child_process";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { serve, sh } from "../../../../dev/app-comparisons/harness/adapters/vite-app.mjs";

const dir = dirname(fileURLToPath(import.meta.url));
const PORT = 4321;
const ORIGIN = `http://127.0.0.1:${PORT}`;
const ENV = {
  APP_ORIGIN: ORIGIN,
  BETTER_AUTH_SECRET: "bench-only-secret-not-for-deployments-0001",
  DATABASE_URL: "postgres://postgres:pass@127.0.0.1:5432/band_chat_postgres",
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
  const stop = await serve(dir, "pnpm exec tsx server/index.ts", `${ORIGIN}/health`, {
    ...ENV,
    NODE_ENV: "production",
    PORT: String(PORT),
  });
  return { url: ORIGIN, stop, rttUrl: `${ORIGIN}/health`, where: "local (Postgres 18 + Hono/Better Auth/pg/ws app server, same machine)" };
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
