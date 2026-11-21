// Comparison harness hooks for the Neon Realtime BandChat (see ../../../../dev/app-comparisons/SPEC.md):
// hosted Neon Postgres with Realtime and Managed Better Auth (aws-us-east-1,
// branch "band-chat"), and the app's Hono server locally (sealing, writes, SPA).
import { readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { ensureNeon } from "../../../../dev/app-comparisons/harness/adapters/neon.mjs";
import { serve, sh } from "../../../../dev/app-comparisons/harness/adapters/vite-app.mjs";

const dir = dirname(fileURLToPath(import.meta.url));
const PORT = 4321;
const ORIGIN = `http://127.0.0.1:${PORT}`;
const neon = () =>
  ensureNeon(dir, { branch: "band-chat", services: ["postgres", "realtime", "auth"], authOrigins: [ORIGIN] });
export const distDir = "dist";

export async function seed() {
  const out = sh(dir, "pnpm exec tsx scripts/seed.ts", { ...neon(), APP_ORIGIN: ORIGIN });
  const ctx = JSON.parse(out.trim().split("\n").at(-1));
  writeFileSync(`${dir}/.bench-ctx.json`, JSON.stringify(ctx));
  return ctx;
}

/** The ctx of the last seed(), for --no-seed runs. */
export const lastSeed = () => JSON.parse(readFileSync(`${dir}/.bench-ctx.json`, "utf8"));

export async function start() {
  const env = neon();
  sh(dir, "pnpm exec vite build --logLevel error", {
    ...env,
    VITE_NEON_REALTIME_URL: env.NEON_REALTIME_URL,
    VITE_NEON_AUTH_URL: env.NEON_AUTH_BASE_URL,
  });
  const stop = await serve(dir, "pnpm exec tsx server/index.ts", `${ORIGIN}/health`, {
    ...env,
    NODE_ENV: "production",
    PORT: String(PORT),
  });
  return {
    url: ORIGIN,
    // A SELECT 1 on Neon through the local app server (see NOTES.md).
    rttUrl: `${ORIGIN}/health`,
    where: "hosted (Neon Postgres + Realtime + Managed Better Auth, aws-us-east-1); Hono app server local",
    stop,
  };
}

/** Signs `page`'s context in as fixture user `key` through the app's sign-in form (Managed Better Auth). */
export async function signIn(page, key, ctx) {
  const user = ctx.users[key];
  await page.goto(ORIGIN);
  await page.getByLabel("Email").fill(user.email);
  await page.getByLabel("Password").fill(user.password);
  // The form reloads the page once signed in: wait for that, then for the app.
  const reloaded = page.waitForNavigation({ timeout: 120_000 });
  await page.getByRole("button", { name: "Sign in", exact: true }).click();
  await reloaded;
  await page.getByLabel("Password").waitFor({ state: "detached", timeout: 120_000 });
}
