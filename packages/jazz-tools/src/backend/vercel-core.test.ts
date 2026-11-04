import { spawn } from "node:child_process";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { createRequire } from "node:module";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { CORE_LAUNCHER } from "./vercel-store.js";

const addon = createRequire(import.meta.url).resolve("jazz-napi");

async function freePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as { port: number };
  await new Promise((resolve) => server.close(resolve));
  return port;
}

// The core a vercel() Sandbox runs: the start script's launcher, with the
// napi addon, serving the production core shell until SIGTERM drains it.
describe("vercel() core launcher", () => {
  const dirs: string[] = [];
  afterEach(() => {
    for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
  });

  it("serves a core until SIGTERM, then stops cleanly", async () => {
    const dataDir = mkdtempSync(join(tmpdir(), "jazz-vercel-core-"));
    dirs.push(dataDir);
    const port = await freePort();
    const options = {
      port,
      dataDir,
      blobs: { kind: "sqlite" },
      trustedWorkload: {
        issuer: "https://oidc.vercel.com/team",
        audience: "https://vercel.com/team",
        subject: "owner:team:project:app:environment:production",
      },
    };
    const appId = "00000000-0000-4000-8000-000000000001";
    const child = spawn(
      process.execPath,
      ["-e", CORE_LAUNCHER, addon, appId, JSON.stringify(options)],
      { stdio: ["ignore", "ignore", "pipe"], env: { ...process.env, NODE_ENV: "production" } },
    );
    let stderr = "";
    child.stderr.on("data", (chunk) => (stderr += chunk));
    const exited = new Promise<number | null>((resolve) => child.on("exit", resolve));
    try {
      const deadline = Date.now() + 20_000;
      for (;;) {
        const ok = await fetch(`http://127.0.0.1:${port}/health`)
          .then((response) => response.ok)
          .catch(() => false);
        if (ok) break;
        if (Date.now() > deadline || child.exitCode !== null) {
          throw new Error(`core never became healthy: ${stderr}`);
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      expect(existsSync(join(dataDir, "jazz.lock"))).toBe(true);
      child.kill("SIGTERM");
      expect(await exited).toBe(0);
    } finally {
      child.kill("SIGKILL");
    }
  }, 40_000);
});
