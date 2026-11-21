// Comparison harness hooks for the Convex StagePlan (see ../../../../dev/app-comparisons/SPEC.md).
// The backend is a local anonymous Convex deployment (`npx convex dev`, no
// account); its data lives in .convex/. seed() starts it if needed, start()
// builds and serves the production app next to it.
import { spawn } from "node:child_process";
import { existsSync, openSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { serve, sh } from "../../../../dev/app-comparisons/harness/adapters/vite-app.mjs";

const dir = dirname(fileURLToPath(import.meta.url));
const PORT = 4402;
const BACKEND = "http://127.0.0.1:3210";
const env = { CONVEX_AGENT_MODE: "anonymous" };
export const distDir = "dist";

let backend;

/** Runs `npx convex dev` (local backend + function push) until stopped. */
async function startBackend() {
  if (backend) return;
  if (!existsSync(`${dir}/node_modules`)) sh(dir, "pnpm install --frozen-lockfile");
  if (!existsSync(`${dir}/.env.local`)) sh(dir, "npx convex dev --once", env);
  // Its output goes to a file: a pipe nobody drains while sh() blocks would stall it.
  const log = openSync(`${dir}/.convex/dev.log`, "w");
  const child = spawn("bash", ["-lc", "exec npx convex dev --tail-logs disable"], {
    cwd: dir,
    env: { ...process.env, ...env },
    stdio: ["ignore", log, log],
    detached: true,
  });
  for (const deadline = Date.now() + 120_000; ; ) {
    if (child.exitCode !== null) throw new Error(`convex dev exited: ${readFileSync(`${dir}/.convex/dev.log`, "utf8")}`);
    if (readFileSync(`${dir}/.convex/dev.log`, "utf8").includes("Convex functions ready")) break;
    if (Date.now() > deadline) throw new Error("convex dev did not get ready");
    await new Promise((r) => setTimeout(r, 200));
  }
  backend = child;
  sh(dir, `node scripts/setup-auth.mjs http://127.0.0.1:${PORT}`);
}

async function stopBackend() {
  if (!backend) return;
  try {
    process.kill(-backend.pid, "SIGTERM");
  } catch {}
  backend = undefined;
  await new Promise((r) => setTimeout(r, 1000));
}

export async function seed() {
  await startBackend();
  const ctx = JSON.parse(sh(dir, "npx convex run seed:run"));
  writeFileSync(`${dir}/.bench-ctx.json`, JSON.stringify(ctx));
  return ctx;
}

/** The ctx of the last seed(), for --no-seed runs. */
export const lastSeed = () => JSON.parse(readFileSync(`${dir}/.bench-ctx.json`, "utf8"));

export async function start() {
  await startBackend();
  sh(dir, "pnpm exec vite build --logLevel error");
  const url = `http://127.0.0.1:${PORT}`;
  const stopApp = await serve(dir, `pnpm exec vite preview --host 127.0.0.1 --port ${PORT} --strictPort`, url);
  return {
    url,
    rttUrl: `${BACKEND}/version`,
    where: "local (anonymous Convex local backend via `npx convex dev`, Vite preview for the app)",
    stop: async () => {
      await stopApp();
      await stopBackend();
    },
  };
}
