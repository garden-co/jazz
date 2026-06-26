import { spawn } from "node:child_process";
import { once } from "node:events";
import { chromium } from "playwright";

const host = "127.0.0.1";
const port = Number(process.env.SMOKE_PORT ?? 4174);
const url = `http://${host}:${port}/`;
const previewReadyText = `http://${host}:${port}/`;

const server = spawn(
  process.platform === "win32" ? "npm.cmd" : "npm",
  ["run", "preview", "--", "--port", String(port), "--strictPort"],
  {
    cwd: new URL("..", import.meta.url),
    env: process.env,
    stdio: ["ignore", "pipe", "pipe"],
  },
);

let serverOutput = "";
let browser;
let rustServer;

server.stdout.setEncoding("utf8");
server.stderr.setEncoding("utf8");
server.stdout.on("data", (chunk) => {
  serverOutput += chunk;
});
server.stderr.on("data", (chunk) => {
  serverOutput += chunk;
});

try {
  await waitForPreview();

  browser = await chromium.launch();
  const page = await browser.newPage();
  page.setDefaultTimeout(30_000);

  await page.goto(url, { waitUntil: "domcontentloaded" });
  await page.waitForFunction(() => document.querySelector("#summary")?.textContent?.includes("Ready"));

  const log = page.locator("#log");
  await expectLog(page, "starting browser WASM worker");
  await expectLog(page, "worker: ready");
  await expectLog(page, "insert write durability: Local");
  await expectLog(page, "update write durability: Local");
  await expectLog(page, "delete write durability: Local");
  await page.waitForFunction(() => document.querySelectorAll("#todosBody .todo-row").length === 0);
  await page.waitForFunction(() => document.querySelector("#rowCount")?.textContent === "0 todos");
  await page.waitForFunction(() => document.querySelector("#runtimeStatus")?.textContent === "Shutdown");
  await page.waitForFunction(() => document.querySelector("#durabilityStatus")?.textContent === "Local");
  await page.waitForFunction(() => document.querySelector("#readStatus")?.textContent === "0 decoded");
  await page.waitForFunction(() => document.querySelector("#watchStatus")?.textContent === "delete: none");
  await page.waitForFunction(() => {
    const rendered = [...document.querySelectorAll("#transitionsBody .transition-row")]
      .map((row) => row.textContent ?? "")
      .join("\n");
    return rendered.includes("initial: none")
      && rendered.includes("insert: Ship direct WasmDb:open")
      && rendered.includes("update: Ship direct WasmDb:done")
      && rendered.includes("delete: none");
  });
  const schemaHex = await page.evaluate(() => window.__jazzBrowserTodoSchemaHex);
  if (typeof schemaHex !== "string" || schemaHex.length === 0) {
    throw new Error("browser bundle did not expose todo schema hex");
  }
  const reloadNamespace = `reload-${crypto.randomUUID()}`;
  await page.goto(`${url}?smoke=reload-write&ns=${encodeURIComponent(reloadNamespace)}`, { waitUntil: "domcontentloaded" });
  await page.waitForFunction(() => document.querySelector("#summary")?.textContent?.includes("Ready"));
  await expectLog(page, "reload insert write durability: Local");

  await page.goto(`${url}?smoke=reload-verify&ns=${encodeURIComponent(reloadNamespace)}`, { waitUntil: "domcontentloaded" });
  await page.waitForFunction(() => document.querySelector("#summary")?.textContent?.includes("Ready"));
  await expectLog(page, "reload update write durability: Local");

  const concurrencyNamespace = `concurrency-${crypto.randomUUID()}`;
  await page.goto(`${url}?smoke=browser-concurrency&ns=${encodeURIComponent(concurrencyNamespace)}`, { waitUntil: "domcontentloaded" });
  await page.waitForFunction(() => document.querySelector("#summary")?.textContent?.includes("Ready"));
  await expectLog(page, "concurrency first worker: browser db opened");
  await expectLog(page, "concurrency second worker: browser db opened");

  const batchNamespace = `batch-${crypto.randomUUID()}`;
  await page.goto(`${url}?smoke=browser-batch-durability&ns=${encodeURIComponent(batchNamespace)}`, { waitUntil: "domcontentloaded" });
  await page.waitForFunction(() => document.querySelector("#summary")?.textContent?.includes("Ready"));
  await expectLog(page, "batch insert Batch durable alpha write durability: Local");
  await expectLog(page, "batch insert Batch durable beta write durability: Local");
  await expectLog(page, "batch insert Batch durable gamma write durability: Local");

  await page.goto(`${url}?smoke=db-all-bytea-order`, { waitUntil: "domcontentloaded" });
  await page.waitForFunction(() => document.querySelector("#summary")?.textContent?.includes("Ready"));
  await expectLog(page, "file insert charlie.bin write durability: Local");

  await page.goto(`${url}?smoke=websocket-boundary`, { waitUntil: "domcontentloaded" });
  await page.waitForFunction(() => document.querySelector("#summary")?.textContent?.includes("Ready"));
  await expectLog(page, "websocket insert write durability: Local");

  rustServer = await startLoopbackWebSocketServer(schemaHex);
  await page.goto(`${url}?smoke=websocket-rust&ws=${encodeURIComponent(rustServer.url)}`, { waitUntil: "domcontentloaded" });
  await page.waitForFunction(() => document.querySelector("#summary")?.textContent?.includes("Ready"));
  await expectLog(page, "websocket rust insert write durability: Local");

  const summaryText = await page.locator("#summary").innerText();
  const logText = await log.innerText();
  console.log(`Smoke passed: ${summaryText}`);
  console.log(logText);
} finally {
  await rustServer?.close();
  await browser?.close();
  await stopServer();
}

async function waitForPreview() {
  const exitPromise = once(server, "exit").then(([code, signal]) => {
    throw new Error(`vite preview exited early with ${signal ?? code}\n${serverOutput}`);
  });

  const readyPromise = new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error(`timed out waiting for vite preview\n${serverOutput}`));
    }, 15_000);
    const check = () => {
      if (serverOutput.includes(previewReadyText)) {
        clearTimeout(timeout);
        resolve();
      }
    };
    server.stdout.on("data", check);
    server.stderr.on("data", check);
    check();
  });

  await Promise.race([readyPromise, exitPromise]);
}

async function expectLog(page, text) {
  await page.waitForFunction(
    ([selector, expected]) => document.querySelector(selector)?.textContent?.includes(expected),
    ["#log", text],
  );
}

async function stopServer() {
  if (server.exitCode !== null || server.signalCode !== null) {
    return;
  }

  server.kill("SIGTERM");
  const timeout = setTimeout(() => server.kill("SIGKILL"), 5_000);
  try {
    await once(server, "exit");
  } finally {
    clearTimeout(timeout);
  }
}

async function startLoopbackWebSocketServer(schemaHex) {
  const child = spawn(
    "cargo",
    [
      "run",
      "-q",
      "-p",
      "jazz-server",
      "--",
      "serve-loopback-websocket-schema",
      schemaHex,
      "--allow-legacy-query-identity",
      "true",
    ],
    {
      cwd: new URL("../../..", import.meta.url),
      stdio: ["pipe", "pipe", "pipe"],
    },
  );
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk) => {
    process.stderr.write(chunk);
  });
  try {
    const rustUrl = await waitForRustUrl(child);
    return {
      url: rustUrl,
      close: () => closeRustServer(child),
    };
  } catch (error) {
    await closeRustServer(child);
    throw error;
  }
}

function waitForRustUrl(child) {
  return new Promise((resolve, reject) => {
    let stdout = "";
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      reject(new Error("timed out waiting for Rust websocket URL"));
    }, 120_000);
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
      const line = stdout.split(/\r?\n/).find((candidate) => candidate.startsWith("ws_url="));
      if (!line || settled) return;
      settled = true;
      clearTimeout(timer);
      resolve(line.slice("ws_url=".length).trim());
    });
    child.on("exit", (code, signal) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(new Error(`Rust websocket server exited before URL code=${code} signal=${signal}`));
    });
    child.on("error", (error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(error);
    });
  });
}

async function closeRustServer(child) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  child.stdin.end();
  const timeout = setTimeout(() => child.kill("SIGKILL"), 5_000);
  try {
    await once(child, "exit");
  } finally {
    clearTimeout(timeout);
  }
}
