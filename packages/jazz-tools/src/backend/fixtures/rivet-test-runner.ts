import { execFileSync, spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { getEnginePath } from "@rivetkit/engine-cli";
import { afterAll, beforeAll, expect, vi } from "vitest";
import { getAvailablePort } from "../../dev/test-helpers.js";

export function rivetTestRunner({ local = false } = {}) {
  if (!local && process.env.JAZZ_RIVET_TEST_ENDPOINT) {
    return {
      endpoint: process.env.JAZZ_RIVET_TEST_ENDPOINT,
      namespace: process.env.RIVET_NAMESPACE ?? "default",
      token: process.env.RIVET_TOKEN ?? "default",
      pool: process.env.RIVET_POOL ?? "jazz",
      secret: process.env.JAZZ_CORE_SECRET!,
    };
  }

  const config = {
    endpoint: "",
    namespace: "default",
    token: "default",
    pool: "jazz",
    secret: randomUUID(),
  };
  let cleanup: (() => Promise<void>) | undefined;
  beforeAll(async () => {
    const binary = getEnginePath();
    execFileSync("cargo", ["build", "--locked", "-p", "jazz-rivet"], { stdio: "inherit" });
    const { target_directory } = JSON.parse(
      execFileSync("cargo", ["metadata", "--format-version=1", "--no-deps"], { encoding: "utf8" }),
    );
    const directory = await mkdtemp(join(tmpdir(), "jazz-rivet-"));
    const port = await getAvailablePort();
    config.endpoint = `http://127.0.0.1:${port}`;
    const runner = spawn(join(target_directory, "debug", "jazz-rivet"), [], {
      cwd: directory,
      env: {
        NODE_ENV: "test",
        PATH: process.env.PATH,
        AWS_ACCESS_KEY_ID: "test-access-key",
        AWS_SECRET_ACCESS_KEY: "test-secret-key",
        RIVET_ENGINE_BINARY_PATH: binary,
        RIVET_RUN_ENGINE_PORT: String(port),
        RIVETKIT_STORAGE_PATH: directory,
        RIVET_POOL: config.pool,
        JAZZ_CORE_SECRET: config.secret,
      },
      stdio: "inherit",
    });
    const closed = once(runner, "close");
    cleanup = async () => {
      runner.kill("SIGTERM");
      try {
        await closed;
      } finally {
        await rm(directory, { recursive: true, force: true });
      }
    };
    await Promise.race([
      closed.then(([code]) => {
        throw new Error(`jazz-rivet exited during startup (${code})`);
      }),
      vi.waitFor(
        async () => {
          const response = await fetch(`${config.endpoint}/actors/names?namespace=default`, {
            headers: { authorization: "Bearer default" },
          });
          expect((await response.json()).names).toHaveProperty("jazz");
        },
        { timeout: 60_000, interval: 250 },
      ),
    ]);
  }, 600_000);
  afterAll(async () => {
    await cleanup?.();
  }, 60_000);
  return config;
}
