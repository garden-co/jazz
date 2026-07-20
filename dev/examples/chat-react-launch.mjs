#!/usr/bin/env node
import { spawn } from "node:child_process";
import { resolve } from "node:path";
import { deploy, startLocalJazzServer } from "../../packages/jazz-tools/dist/testing/index.js";

const rootDir = resolve(import.meta.dirname, "../..");
const exampleDir = resolve(rootDir, "examples/chat-react");

const appId = process.env.JAZZ_CHAT_APP_ID ?? "00000000-0000-0000-0000-000000000421";
const serverPort = Number(process.env.JAZZ_CHAT_SERVER_PORT ?? "4210");
const appPort = Number(process.env.JAZZ_CHAT_APP_PORT ?? "5175");
const adminSecret = process.env.JAZZ_CHAT_ADMIN_SECRET ?? "chat-react-admin";
const backendSecret = process.env.JAZZ_CHAT_BACKEND_SECRET ?? adminSecret;
const modeArg = process.argv.includes("--sync-mode")
  ? "sync"
  : process.argv.includes("--async-mode")
    ? "async"
    : undefined;
const subscriptionMode = modeArg ?? process.env.JAZZ_CHAT_SUBSCRIPTION_MODE ?? "async";

if (!["async", "sync"].includes(subscriptionMode)) {
  throw new Error("JAZZ_CHAT_SUBSCRIPTION_MODE must be 'async' or 'sync'.");
}

const server = await startLocalJazzServer({
  appId,
  port: serverPort,
  adminSecret,
  backendSecret,
  inMemory: true,
  allowLocalFirstAuth: true,
  enableLogs: true,
});

let vite;
let shuttingDown = false;

async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  if (signal) {
    console.log(`[chat-react] received ${signal}; shutting down`);
  }
  if (vite && !vite.killed) {
    vite.kill("SIGTERM");
  }
  await server.stop();
}

process.once("SIGINT", () => void shutdown("SIGINT"));
process.once("SIGTERM", () => void shutdown("SIGTERM"));

try {
  await deploy({
    appId,
    serverUrl: server.url,
    adminSecret,
    schemaDir: exampleDir,
  });

  const env = {
    ...process.env,
    VITE_JAZZ_APP_ID: appId,
    VITE_JAZZ_SERVER_URL: server.url,
    VITE_JAZZ_SUBSCRIPTION_MODE: subscriptionMode,
    JAZZ_ADMIN_SECRET: adminSecret,
    JAZZ_CHAT_APP_ID: appId,
    JAZZ_CHAT_SERVER_URL: server.url,
    JAZZ_CHAT_ADMIN_SECRET: adminSecret,
    JAZZ_CHAT_BACKEND_SECRET: backendSecret,
  };

  console.log(`[chat-react] app:  http://127.0.0.1:${appPort}`);
  console.log(`[chat-react] sync: ${server.url}/sync`);
  console.log(`[chat-react] subscription_mode=${subscriptionMode}`);
  console.log(
    `[chat-react] browser receipt: node dev/examples/chat-react-browser-receipt.mjs --${subscriptionMode}-mode`,
  );

  vite = spawn("pnpm", ["dev", "--host", "127.0.0.1", "--port", String(appPort)], {
    cwd: exampleDir,
    env,
    stdio: "inherit",
  });

  const exitCode = await new Promise((resolveExit) => {
    vite.once("exit", (code, signal) => {
      if (signal) resolveExit(0);
      else resolveExit(code ?? 0);
    });
  });
  await shutdown();
  process.exit(exitCode);
} catch (error) {
  console.error("[chat-react] launch failed:", error);
  await shutdown();
  process.exit(1);
}
