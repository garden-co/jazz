// Comparison harness hooks for the plain-Postgres Wequencer (see ../../../../dev/app-comparisons/SPEC.md):
// local Postgres 18 and the Next app on its custom server (Better Auth, the
// Hono API routes, the live WebSocket).
import { execSync } from "node:child_process";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { serve, sh } from "../../../../dev/app-comparisons/harness/adapters/vite-app.mjs";

const dir = dirname(fileURLToPath(import.meta.url));
const PORT = 4331;
const LIVE_PORT = PORT + 1;
const ORIGIN = `http://127.0.0.1:${PORT}`;
const ENV = {
  NEXT_PUBLIC_APP_ORIGIN: ORIGIN,
  NEXT_PUBLIC_LIVE_PORT: String(LIVE_PORT),
  BETTER_AUTH_SECRET: "bench-only-secret-not-for-deployments-0003",
  DATABASE_URL: "postgres://postgres:pass@127.0.0.1:5432/wequencer_postgres",
};
export const distDir = ".next/static";

function prepare() {
  if (!existsSync(`${dir}/node_modules/.bin/next`)) {
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
  sh(dir, "pnpm exec next build > /tmp/wequencer-postgres-build.log", { ...ENV, NODE_ENV: "production" });
  const env = { ...ENV, NODE_ENV: "production", LIVE_PORT: String(LIVE_PORT) };
  const stopLive = await serve(dir, "pnpm exec tsx live-server.ts", `http://127.0.0.1:${LIVE_PORT}/`, env);
  const stopNext = await serve(dir, `pnpm exec next start -H 127.0.0.1 -p ${PORT}`, `${ORIGIN}/api/health`, env);
  return {
    url: ORIGIN,
    rttUrl: `${ORIGIN}/api/health`,
    where: "local (Postgres 18 + Next/Better Auth/Hono API routes + a ws server fed by LISTEN/NOTIFY, same machine)",
    stop: async () => {
      await stopNext();
      await stopLive();
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
