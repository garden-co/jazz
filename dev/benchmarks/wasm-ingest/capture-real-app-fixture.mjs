#!/usr/bin/env node
import { existsSync } from "node:fs";
import { mkdir, readdir, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { chromium } from "playwright";

const baseUrl = process.env.JAZZ_WASM_INGEST_APP_URL ?? "http://localhost:3000";
const email = process.env.JAZZ_WASM_INGEST_LOGIN_EMAIL ?? "";
const outDir = resolve(
  process.env.JAZZ_WASM_INGEST_CAPTURE_DIR ?? "/tmp/jazz-wasm-ingest-captures",
);
const outFile = resolve(
  process.env.JAZZ_WASM_INGEST_CAPTURE_FILE ??
    join(outDir, `wasm-ingest-fixture-${timestamp()}.json`),
);
const holderReadyTimeoutMs = Number(process.env.JAZZ_WASM_INGEST_READY_TIMEOUT_MS ?? "120000");
const wsUrlPattern = new RegExp(process.env.JAZZ_WASM_INGEST_WS_PATTERN ?? "/apps/.*/ws");

await mkdir(outDir, { recursive: true });

if (!email) {
  throw new Error("JAZZ_WASM_INGEST_LOGIN_EMAIL must be set");
}

const executablePath = await findCachedChromium();
const browser = await chromium.launch({ headless: true, executablePath });
const startedAt = new Date().toISOString();

try {
  const auth = await authenticate(browser);
  const context = await browser.newContext({ viewport: { width: 1440, height: 960 } });
  await context.addCookies(auth.cookies);
  const page = await context.newPage();
  await installWorkerCapture(page);

  const websockets = [];
  page.on("websocket", (ws) => {
    if (!wsUrlPattern.test(ws.url())) return;
    const socket = {
      url: ws.url(),
      openedAtEpochMs: Date.now(),
      sent: [],
      received: [],
      closedAtEpochMs: null,
    };
    websockets.push(socket);
    ws.on("framesent", ({ payload }) => {
      socket.sent.push(framePayload(payload));
    });
    ws.on("framereceived", ({ payload }) => {
      socket.received.push(framePayload(payload));
    });
    ws.on("close", () => {
      socket.closedAtEpochMs = Date.now();
    });
  });

  const url = `${baseUrl}/?codexWasmCapture=1&codexTs=${Date.now()}`;
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
  await waitForReady(page);

  const workerCapture = await page.evaluate(() => globalThis.__jazzWasmIngestCapture ?? null);
  const appSnapshot = await collectPerfSnapshot(page);
  await context.close();

  const fixture = {
    version: 1,
    kind: "jazz-wasm-ingest-fixture",
    capturedAt: startedAt,
    finishedAt: new Date().toISOString(),
    baseUrl,
    url,
    executablePath,
    wsUrlPattern: wsUrlPattern.source,
    workerCapture,
    websockets,
    appSnapshot: {
      allReady: appSnapshot.allReady,
      holderReadiness: appSnapshot.holderReadiness,
      perfTail: appSnapshot.perfTail,
    },
  };
  await writeFile(outFile, `${JSON.stringify(fixture, null, 2)}\n`, { mode: 0o600 });
  console.log(
    JSON.stringify({
      ok: true,
      outFile,
      subscriptions: workerCapture?.subscriptions?.length ?? 0,
      receivedBatches: websockets.reduce((sum, ws) => sum + ws.received.length, 0),
      receivedBytes: websockets.reduce(
        (sum, ws) => sum + ws.received.reduce((inner, frame) => inner + frame.length, 0),
        0,
      ),
      allReady: appSnapshot.allReady ?? null,
    }),
  );
} finally {
  await browser.close();
}

async function installWorkerCapture(page) {
  await page.addInitScript(() => {
    const capture = {
      open: null,
      connect: null,
      subscriptions: [],
      workerRequests: [],
    };
    const originalPostMessage = Worker.prototype.postMessage;
    Worker.prototype.postMessage = function patchedPostMessage(message, transfer) {
      try {
        if (message && typeof message === "object" && typeof message.method === "string") {
          const method = message.method;
          capture.workerRequests.push({
            id: message.id,
            method,
            atMs: performance.now(),
          });
          if (method === "open") {
            const [, dbName, schema, node, author] = message.args ?? [];
            capture.open = {
              id: message.id,
              atMs: performance.now(),
              dbName,
              schema,
              node: bytesBase64(node),
              author: bytesBase64(author),
            };
          } else if (method === "connect") {
            const [url, authJson] = message.args ?? [];
            capture.connect = { id: message.id, atMs: performance.now(), url, authJson };
          } else if (method === "createExecutedSubscription") {
            const [ownerHandle, queryJson, sessionJson, tier, optionsJson] = message.args ?? [];
            capture.subscriptions.push({
              id: message.id,
              atMs: performance.now(),
              ownerHandle,
              queryJson,
              sessionJson,
              tier,
              optionsJson,
            });
          }
        }
      } catch (error) {
        capture.workerRequests.push({
          method: "capture-error",
          atMs: performance.now(),
          error: String(error?.message ?? error),
        });
      }
      return originalPostMessage.call(this, message, transfer);
    };
    globalThis.__jazzWasmIngestCapture = capture;

    function bytesBase64(value) {
      if (!value) return null;
      const bytes = value instanceof Uint8Array ? value : new Uint8Array(value);
      let binary = "";
      const chunkSize = 0x8000;
      for (let offset = 0; offset < bytes.length; offset += chunkSize) {
        binary += String.fromCharCode(...bytes.subarray(offset, offset + chunkSize));
      }
      return btoa(binary);
    }
  });
}

async function authenticate(browser) {
  const context = await browser.newContext({ viewport: { width: 1440, height: 960 } });
  const page = await context.newPage();
  await page.goto(baseUrl, { waitUntil: "domcontentloaded", timeout: 30000 });
  const magicResponse = await page.evaluate(async (loginEmail) => {
    const response = await fetch("/request", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email: loginEmail }),
    });
    return {
      status: response.status,
      body: await response.json().catch(() => null),
    };
  }, email);
  if (magicResponse.status !== 200 || !magicResponse.body?.code) {
    throw new Error(`Dev OTP request did not return a code: ${JSON.stringify(magicResponse)}`);
  }
  const codeUrl = new URL(baseUrl);
  codeUrl.searchParams.set("code", magicResponse.body.code);
  await page.goto(codeUrl.toString(), { waitUntil: "domcontentloaded", timeout: 30000 });
  await page.waitForFunction((expectedEmail) => globalThis.user?.email === expectedEmail, email, {
    timeout: 30000,
  });
  const cookies = await context.cookies(baseUrl);
  const user = await page.evaluate(() => globalThis.user ?? null);
  await context.close();
  return { cookies, user, magicResponse };
}

async function waitForReady(page) {
  const handle = await page.waitForFunction(
    () =>
      (globalThis.__codexJazzPerf ?? []).some((entry) => entry.label === "[jazz-holder] all-ready"),
    undefined,
    { timeout: holderReadyTimeoutMs },
  );
  await handle.dispose();
}

async function collectPerfSnapshot(page) {
  return await page.evaluate(() => {
    const perf = globalThis.__codexJazzPerf ?? [];
    const allReady =
      perf.findLast?.((entry) => entry.label === "[jazz-holder] all-ready") ??
      perf.filter((entry) => entry.label === "[jazz-holder] all-ready").at(-1) ??
      null;
    return {
      allReady,
      holderReadiness: globalThis.__codexJazzHolderReadiness ?? null,
      perfTail: perf.slice(-100),
    };
  });
}

function framePayload(payload) {
  if (typeof payload === "string") {
    return {
      kind: "text",
      atEpochMs: Date.now(),
      length: Buffer.byteLength(payload),
      text: payload,
    };
  }
  const buffer = Buffer.isBuffer(payload) ? payload : Buffer.from(payload);
  return {
    kind: "binary",
    atEpochMs: Date.now(),
    length: buffer.byteLength,
    base64: buffer.toString("base64"),
  };
}

async function findCachedChromium() {
  if (process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE) return process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE;
  const cacheDir = resolve(
    process.env.PLAYWRIGHT_BROWSERS_PATH ?? `${process.env.HOME}/Library/Caches/ms-playwright`,
  );
  if (!existsSync(cacheDir)) return undefined;
  const entries = await readdir(cacheDir).catch(() => []);
  const candidates = entries
    .filter((entry) => entry.startsWith("chromium_headless_shell-"))
    .sort()
    .reverse()
    .map((entry) =>
      join(cacheDir, entry, "chrome-headless-shell-mac-arm64", "chrome-headless-shell"),
    );
  return candidates.find((candidate) => existsSync(candidate));
}

function timestamp() {
  return new Date()
    .toISOString()
    .replace(/[-:]/g, "")
    .replace(/\.\d{3}Z$/, "Z");
}
