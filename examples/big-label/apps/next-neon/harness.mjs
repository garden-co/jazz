// Comparison harness hooks for the Neon Realtime BigLabel (see ../../../../dev/app-comparisons/SPEC.md):
// hosted Neon Postgres with Realtime and Managed Better Auth (aws-us-east-1,
// branch "big-label"), and the Next app locally (auth proxy, sealing route,
// write Server Action).
import { readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { ensureNeon } from "../../../../dev/app-comparisons/harness/adapters/neon.mjs";
import { serve, sh } from "../../../../dev/app-comparisons/harness/adapters/vite-app.mjs";

const dir = dirname(fileURLToPath(import.meta.url));
const PORT = 4333;
const ORIGIN = `http://127.0.0.1:${PORT}`;
const neon = () =>
  ensureNeon(dir, { branch: "big-label", services: ["postgres", "realtime", "auth"], authOrigins: [ORIGIN] });
const env = () => {
  const e = neon();
  return {
    ...e,
    NEXT_PUBLIC_NEON_REALTIME_URL: e.NEON_REALTIME_URL,
    // Signs Managed Better Auth's session-data cookie on the app's origin.
    NEON_AUTH_COOKIE_SECRET: "bench-only-cookie-secret-not-for-deployments-0004",
    APP_ORIGIN: ORIGIN,
  };
};
export const distDir = ".next/static";

export async function seed() {
  const out = sh(dir, "pnpm exec tsx --conditions react-server scripts/seed.ts", env());
  const ctx = JSON.parse(out.trim().split("\n").at(-1));
  writeFileSync(`${dir}/.bench-ctx.json`, JSON.stringify(ctx));
  return ctx;
}

/** The ctx of the last seed(), for --no-seed runs. */
export const lastSeed = () => JSON.parse(readFileSync(`${dir}/.bench-ctx.json`, "utf8"));

export async function start() {
  const e = env();
  sh(dir, "pnpm exec next build > /tmp/big-label-neon-build.log", { ...e, NODE_ENV: "production" });
  const stop = await serve(dir, `pnpm exec next start -H 127.0.0.1 -p ${PORT}`, `${ORIGIN}/`, { ...e, NODE_ENV: "production" });
  return {
    url: ORIGIN,
    // A SELECT 1 on Neon through the Next server (see NOTES.md).
    rttUrl: `${ORIGIN}/api/health`,
    where: "hosted (Neon Postgres + Realtime + Managed Better Auth, aws-us-east-1); Next server local",
    stop,
  };
}

/** Signs `page`'s context in as fixture user `key` through Managed Better Auth's API on the app's origin. */
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
