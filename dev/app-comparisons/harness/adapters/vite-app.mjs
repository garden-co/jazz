// Shared helpers for app harness.mjs files: run a command, serve a built app.
import { spawn, execFileSync } from "node:child_process";

export function sh(cwd, cmd, env = {}) {
  return execFileSync("bash", ["-lc", cmd], { cwd, env: { ...process.env, ...env }, stdio: ["ignore", "pipe", "inherit"] }).toString();
}

/** Start a long-running server command; resolves once `url` answers. */
export async function serve(cwd, cmd, url, env = {}, timeoutMs = 60_000) {
  const child = spawn("bash", ["-lc", `exec ${cmd}`], { cwd, env: { ...process.env, ...env }, stdio: ["ignore", "inherit", "inherit"], detached: true });
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (child.exitCode !== null) throw new Error(`${cmd} exited with ${child.exitCode}`);
    try {
      const r = await fetch(url);
      if (r.status < 500) break;
    } catch {}
    if (Date.now() > deadline) throw new Error(`${url} did not come up`);
    await new Promise((r) => setTimeout(r, 250));
  }
  return async () => {
    try {
      process.kill(-child.pid, "SIGTERM");
    } catch {}
    await new Promise((r) => setTimeout(r, 500));
  };
}
