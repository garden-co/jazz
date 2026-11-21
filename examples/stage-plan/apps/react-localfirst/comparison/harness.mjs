// Comparison harness hooks for the Jazz StagePlan (see dev/app-comparisons/README.md).
// Variants: "default" (as shipped: starts thin, upgrades to local-first in the
// background), "now" (localFirst: "now": local-first from the first load),
// "thin" (VITE_JAZZ_THIN=1: the thin client only).
import { readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { serve, sh } from "../../../../../dev/app-comparisons/harness/adapters/vite-app.mjs";

// The app directory (these hooks sit in its comparison/ folder).
const dir = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const DATA = "comparison/.bench-data";
const PORT = 4401;
export const distDir = "dist";

export async function seed() {
  rmSync(`${dir}/${DATA}`, { recursive: true, force: true });
  const out = sh(dir, "pnpm exec tsx comparison/seed.ts", { JAZZ_DATA_DIR: DATA });
  const ctx = JSON.parse(out.trim().split("\n").at(-1));
  writeFileSync(`${dir}/comparison/.bench-ctx.json`, JSON.stringify(ctx));
  return ctx;
}

/** The ctx of the last seed(), for --no-seed runs. */
export const lastSeed = () => JSON.parse(readFileSync(`${dir}/comparison/.bench-ctx.json`, "utf8"));

export async function start({ variant = "default" } = {}) {
  sh(dir, "pnpm exec vite build --logLevel error", {
    VITE_JAZZ_THIN: variant === "thin" ? "1" : "",
    VITE_JAZZ_LOCAL_FIRST: variant === "now" ? "now" : "",
  });
  const url = `http://127.0.0.1:${PORT}`;
  const stop = await serve(dir, `pnpm exec vite preview --host 127.0.0.1 --port ${PORT} --strictPort`, url, { JAZZ_DATA_DIR: DATA });
  return { url, stop, where: "local (Jazz backend in the Vite preview server)" };
}
