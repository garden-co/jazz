#!/usr/bin/env node
// Runs an example that talks to a Jazz core directly (no createJazz backend):
// starts a local core, deploys the example's schema and permissions to it,
// then runs the given command with the core's URL and app id in its env.
//
//   node ../../dev/examples/with-local-core.mjs vite --port 5173
//
// Monorepo-only stand-in for the removed dev plugins' managed server. The
// createJazz hero examples don't need it: Jazz runs inside their own backend.
// Data lives in .jazz/ next to the example; JAZZ_IN_MEMORY=1 keeps it in memory.
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { deploy } from "../../packages/jazz-tools/dist/dev/catalogue-project.js";
import { startLocalJazzServer } from "../../packages/jazz-tools/dist/testing/index.js";

const exampleDir = process.cwd();
const command = process.argv.slice(2);
if (command.length === 0) {
  console.error("usage: with-local-core.mjs <command> [args...]");
  process.exit(2);
}

// A caller that already runs a core (e.g. dev/examples/*-launch.mjs) passes its URL.
if (process.env.VITE_JAZZ_SERVER_URL || process.env.NEXT_PUBLIC_JAZZ_SERVER_URL) {
  const child = spawn(command[0], command.slice(1), { stdio: "inherit" });
  child.once("exit", (code, signal) => process.exit(signal ? 0 : (code ?? 0)));
} else {
  await runWithLocalCore();
}

async function runWithLocalCore() {
  const inMemory = process.env.JAZZ_IN_MEMORY === "1";
  const dataDir = join(exampleDir, ".jazz");
  mkdirSync(dataDir, { recursive: true });

  function keptAppId() {
    const explicit =
      process.env.VITE_JAZZ_APP_ID ||
      process.env.NEXT_PUBLIC_JAZZ_APP_ID ||
      process.env.JAZZ_APP_ID;
    if (explicit) return explicit;
    const file = join(dataDir, "core-app-id");
    try {
      return readFileSync(file, "utf8").trim();
    } catch {
      const id = randomUUID();
      writeFileSync(file, `${id}\n`);
      return id;
    }
  }

  const appId = keptAppId();
  const adminSecret = process.env.JAZZ_ADMIN_SECRET || randomUUID();
  const backendSecret = process.env.BACKEND_SECRET || randomUUID();

  const server = await startLocalJazzServer({
    appId,
    port: Number(process.env.JAZZ_CORE_PORT ?? 0) || undefined,
    adminSecret,
    backendSecret,
    allowLocalFirstAuth: true,
    ...(inMemory ? { inMemory: true } : { dataDir: join(dataDir, "core") }),
  });

  let child;
  async function shutdown(code) {
    if (child && child.exitCode === null) child.kill("SIGTERM");
    await server.stop().catch(() => undefined);
    process.exit(code);
  }
  process.once("SIGINT", () => void shutdown(0));
  process.once("SIGTERM", () => void shutdown(0));

  try {
    await deploy({ appId, serverUrl: server.url, adminSecret, schemaDir: exampleDir });
  } catch (error) {
    console.error("[with-local-core] deploy failed:", error);
    await shutdown(1);
  }

  console.log(`[with-local-core] core ${server.url} app ${appId}`);
  child = spawn(command[0], command.slice(1), {
    stdio: "inherit",
    env: {
      ...process.env,
      JAZZ_APP_ID: appId,
      JAZZ_SERVER_URL: server.url,
      JAZZ_ADMIN_SECRET: adminSecret,
      BACKEND_SECRET: backendSecret,
      VITE_JAZZ_APP_ID: appId,
      VITE_JAZZ_SERVER_URL: server.url,
      NEXT_PUBLIC_JAZZ_APP_ID: appId,
      NEXT_PUBLIC_JAZZ_SERVER_URL: server.url,
      EXPO_PUBLIC_JAZZ_APP_ID: appId,
      EXPO_PUBLIC_JAZZ_SERVER_URL: server.url,
    },
  });
  child.once("exit", (code, signal) => void shutdown(signal ? 0 : (code ?? 0)));
}
