// Comparison harness hooks for the Convex Wequencer (see ../../../../dev/app-comparisons/SPEC.md).
// The backend is a local anonymous Convex deployment (`npx convex dev`, no
// account); its data lives in .convex/. seed() starts it if needed, start()
// builds the Next app (`next build`) and serves it with `next start`.
// Variants: "default" (pads wait for the server), "optimistic" (pad toggles
// use a Convex optimistic update, like the docs' counter example).
import { spawn } from "node:child_process";
import { existsSync, openSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { serve, sh } from "../../../../dev/app-comparisons/harness/adapters/vite-app.mjs";

const dir = dirname(fileURLToPath(import.meta.url));
const PORT = 4404;
const BACKEND = "http://127.0.0.1:3210";
const env = { CONVEX_AGENT_MODE: "anonymous" };
export const distDir = ".next/static";

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
  const ctx = JSON.parse(sh(dir, "node scripts/seed.mjs").trim().split("\n").at(-1));
  writeFileSync(`${dir}/.bench-ctx.json`, JSON.stringify(ctx));
  return ctx;
}

/** The ctx of the last seed(), for --no-seed runs. */
export const lastSeed = () => JSON.parse(readFileSync(`${dir}/.bench-ctx.json`, "utf8"));

export async function start({ variant = "default" } = {}) {
  await startBackend();
  const optimistic = variant === "optimistic" ? "1" : "";
  sh(dir, "pnpm exec next build > /tmp/wequencer-convex-build.log", { NEXT_PUBLIC_CONVEX_OPTIMISTIC_PADS: optimistic });
  const url = `http://127.0.0.1:${PORT}`;
  const stopApp = await serve(dir, `pnpm exec next start -H 127.0.0.1 -p ${PORT}`, `${url}/`, { NODE_ENV: "production" });
  return {
    url,
    rttUrl: `${BACKEND}/version`,
    where: "local (anonymous Convex local backend via `npx convex dev`, `next start` for the app)",
    stop: async () => {
      await stopApp();
      await stopBackend();
    },
  };
}

/** Signs `page`'s context in as fixture user `key` through Convex Auth's Next.js route (untimed). */
export async function signIn(page, key, ctx) {
  const user = ctx.users[key];
  const res = await page.context().request.post(`http://127.0.0.1:${PORT}/api/auth`, {
    data: {
      action: "auth:signIn",
      args: { provider: "password", params: { flow: "signIn", email: user.email, password: user.password } },
    },
  });
  if (!res.ok()) throw new Error(`sign-in failed: ${res.status()} ${await res.text()}`);
}
