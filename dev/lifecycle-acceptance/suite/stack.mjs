// The server side, all from the candidate: the core binary (release-server), a stock createJazz
// backend on it (release-npm), and t9's OIDC issuer for users.
import { spawn } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import { chmodSync } from "node:fs";
import { createServer } from "node:net";
import { join } from "node:path";
import { WORK } from "../lib/common.mjs";
import { cleanEnv } from "../build/consumer.mjs";
import { redact } from "./redact.mjs";

function freePort() {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address();
      server.close(() => resolve(port));
    });
  });
}

export async function startStack() {
  const npm = join(WORK, "npm");
  const children = [];
  const stop = () => {
    for (const child of children) if (child.exitCode === null) child.kill("SIGKILL"); // by PID
  };
  const start = async (name, command, args, env, up) => {
    const child = spawn(command, args, { cwd: npm, env: cleanEnv(env), stdio: ["ignore", "pipe", "pipe"] });
    let log = "";
    for (const stream of [child.stdout, child.stderr])
      stream.on("data", (bytes) => (log = (log + bytes).slice(-6000)));
    child.on("error", (error) => (log += error.message));
    children.push(child);
    const until = Date.now() + 60_000;
    while (Date.now() < until && child.exitCode === null) {
      try {
        const response = await fetch(up, { signal: AbortSignal.timeout(2000) });
        if (response.ok) return response;
      } catch {
        // not up yet
      }
      await new Promise((resolve) => setTimeout(resolve, 200));
    }
    stop();
    throw new Error(`${name} didn't start: ${redact(log).slice(-1500)}`);
  };

  const [corePort, oidcPort, backendPort] = [await freePort(), await freePort(), await freePort()];
  const appId = randomUUID();
  const secret = randomBytes(32).toString("hex");
  const binary = join(WORK, "artifacts", "release-server", `jazz-tools-linux-${process.arch === "arm64" ? "arm64" : "x64"}`);
  chmodSync(binary, 0o755);
  await start(
    "core (release-server)",
    binary,
    ["server", appId, "--port", String(corePort), "--in-memory", "--admin-secret", secret],
    {},
    `http://127.0.0.1:${corePort}/health`,
  );
  const issuer = `http://127.0.0.1:${oidcPort}`;
  await start("OIDC issuer", process.execPath, ["--import", "tsx", "oidc.ts"], { OIDC_PORT: String(oidcPort) }, `${issuer}/jwks`);
  const up = await start(
    "createJazz backend",
    process.execPath,
    ["--import", "tsx", join(npm, "server.ts")],
    {
      PORT: String(backendPort),
      APP_DIR: join(npm, "generated", "backend"),
      APP_ID: appId,
      CORE_URL: `http://127.0.0.1:${corePort}`,
      CORE_SECRET: secret,
      USERS_OIDC: issuer,
    },
    `http://127.0.0.1:${backendPort}/up`,
  );
  const served = await up.json();
  if (served.appId !== appId) {
    stop();
    throw new Error(`the backend serves ${served.appId}, not ${appId}`);
  }
  return {
    appId,
    backendPort,
    stop,
    async token(user) {
      const query = new URLSearchParams({ sub: user, aud: "jazz:stageplan-users" });
      return (await fetch(`${issuer}/token?${query}`, { signal: AbortSignal.timeout(5000) })).text();
    },
  };
}
