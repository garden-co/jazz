// Comparison harness hooks for the Jazz BandChat (see dev/app-comparisons/README.md).
// Variants: "default" (as shipped: thin first, local-first in the background), "now" (localFirst: "now"), "thin".
import { readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { serve, sh } from "../../../../../dev/app-comparisons/harness/adapters/vite-app.mjs";

// The app directory (these hooks sit in its comparison/ folder).
const dir = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const PORT = 4402;
const ORIGIN = `http://127.0.0.1:${PORT}`;
const ENV = {
  APP_ORIGIN: ORIGIN,
  JAZZ_APP_ID: "00000000-0000-0000-0000-0000000b0c4a",
  BETTER_AUTH_SECRET: "bench-only-secret-not-for-deployments-0001",
  JAZZ_DATA_PATH: "comparison/.bench-data",
};
export const distDir = "dist";

export async function seed() {
  rmSync(`${dir}/comparison/.bench-data`, { recursive: true, force: true });
  const out = sh(dir, "pnpm exec tsx comparison/seed.ts", ENV);
  const ctx = JSON.parse(out.trim().split("\n").at(-1));
  writeFileSync(`${dir}/comparison/.bench-ctx.json`, JSON.stringify(ctx));
  return ctx;
}

/** The ctx of the last seed(), for --no-seed runs. */
export const lastSeed = () => JSON.parse(readFileSync(`${dir}/comparison/.bench-ctx.json`, "utf8"));

export async function start({ variant = "default" } = {}) {
  sh(dir, "node scripts/check-config.mjs && pnpm exec vite build --logLevel error", {
    ...ENV,
    VITE_JAZZ_THIN: variant === "thin" ? "1" : "",
    VITE_JAZZ_LOCAL_FIRST: variant === "now" ? "now" : "",
  });
  const stop = await serve(dir, "pnpm exec tsx server/index.ts", `${ORIGIN}/health`, { ...ENV, NODE_ENV: "production", PORT: String(PORT) });
  return { url: ORIGIN, stop, where: "local (Jazz in the app's Hono server)", rttUrl: `${ORIGIN}/health` };
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
