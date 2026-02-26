import { spawn } from "node:child_process";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = resolve(__dirname, "..");
const binary = resolve(root, "../../target/debug/jazz-tools");
const schemaDir = resolve(root, "schema");

const APP_ID = "00000000-0000-0000-0000-000000000099";
const PORT = 4180;
const ADMIN_SECRET = "repro-admin-secret";

const dataDir = mkdtempSync(resolve(tmpdir(), "policy-bypass-repro-"));

function cleanup() {
  try {
    rmSync(dataDir, { recursive: true, force: true });
  } catch {}
}
process.on("exit", cleanup);
process.on("SIGINT", () => {
  cleanup();
  process.exit(130);
});
process.on("SIGTERM", () => {
  cleanup();
  process.exit(143);
});

// 1. Start the jazz server
const server = spawn(
  binary,
  [
    "server",
    APP_ID,
    "--port",
    String(PORT),
    "--data-dir",
    dataDir,
    "--admin-secret",
    ADMIN_SECRET,
    "--allow-demo",
  ],
  { stdio: ["ignore", "pipe", "pipe"] },
);

server.stdout.on("data", (chunk) => process.stdout.write(`[server] ${chunk}`));
server.stderr.on("data", (chunk) => process.stderr.write(`[server] ${chunk}`));
server.on("exit", (code) => {
  console.error(`[server] exited with code ${code}`);
  process.exit(1);
});

// 2. Wait for health check
async function waitForHealth() {
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`http://127.0.0.1:${PORT}/health`);
      if (res.ok) return;
    } catch {}
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error("Server did not become healthy in 15s");
}

await waitForHealth();
console.log(`[dev] Server healthy on port ${PORT}`);

// 3. Push schema
await new Promise((resolve, reject) => {
  const push = spawn(
    binary,
    [
      "schema:push",
      APP_ID,
      "--server-url",
      `http://127.0.0.1:${PORT}`,
      "--admin-secret",
      ADMIN_SECRET,
      "--schema-dir",
      schemaDir,
      "--env",
      "dev",
      "--user-branch",
      "main",
    ],
    { stdio: ["ignore", "pipe", "pipe"] },
  );

  push.stdout.on("data", (chunk) => process.stdout.write(`[schema] ${chunk}`));
  push.stderr.on("data", (chunk) => process.stderr.write(`[schema] ${chunk}`));
  push.on("exit", (code) =>
    code === 0 ? resolve() : reject(new Error(`schema:push exited ${code}`)),
  );
});
console.log("[dev] Schema pushed");

// 4. Start vite
const vite = spawn("npx", ["vite"], {
  cwd: root,
  stdio: "inherit",
  env: {
    ...process.env,
    VITE_JAZZ_SERVER_URL: `http://127.0.0.1:${PORT}`,
    VITE_JAZZ_APP_ID: APP_ID,
  },
});

vite.on("exit", (code) => {
  server.kill("SIGTERM");
  process.exit(code ?? 0);
});
