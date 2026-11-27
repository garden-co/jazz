// Comparison harness hooks for the Jazz BigLabel (see dev/app-comparisons/README.md).
// Variants: "default" (thin first, local-first in the background, as the other
// Jazz heroes ship), "now" (localFirst: "now"), "thin" (thin client only:
// BigLabel upstream as shipped).
import { readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { serve, sh } from "../../../dev/app-comparisons/harness/adapters/vite-app.mjs";

// The app directory (these hooks sit in its comparison/ folder).
const dir = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const PORT = 4404;
const ORIGIN = `http://127.0.0.1:${PORT}`;
const ENV = {
  NEXT_PUBLIC_APP_ORIGIN: ORIGIN,
  JAZZ_APP_ID: "00000000-0000-0000-0000-0000000b1a8e",
  BETTER_AUTH_SECRET: "bench-only-secret-not-for-deployments-0004",
  JAZZ_DATA_PATH: "comparison/.bench-data",
};
export const distDir = ".next/static";

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
  sh(dir, "pnpm exec next build > /tmp/big-label-jazz-build.log", {
    ...ENV,
    NODE_ENV: "production",
    // Unset (variant "thin") builds BigLabel as shipped: the thin client only.
    NEXT_PUBLIC_JAZZ_LOCAL_FIRST: { default: "background", now: "now" }[variant] ?? "",
  });
  const stop = await serve(dir, `pnpm exec next start -H 127.0.0.1 -p ${PORT}`, `${ORIGIN}/`, { ...ENV, NODE_ENV: "production" });
  return { url: ORIGIN, stop, where: "local (Jazz inside the Next server)" };
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
