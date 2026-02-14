#!/usr/bin/env node

const fs = require("fs");
const http = require("http");
const path = require("path");

function loadPlaywright() {
  try {
    return require("playwright");
  } catch {
    const fallback = path.join(__dirname, "..", "node_modules", "playwright");
    return require(fallback);
  }
}

const { chromium } = loadPlaywright();

const DEFAULT_COUNT = 5000;
const DEFAULT_VALUE_SIZES = [32, 256, 4096];
const DEFAULT_PROFILE = "basic";
const DEFAULT_SEED = "0xA5A5A5A501234567";
const DEFAULT_CACHE_MB = 32;
const DEFAULT_PIN_INTERNAL_PAGES = true;

function parseSeed(raw) {
  const text = String(raw || "").trim();
  if (!text) {
    throw new Error("`--seed` must not be empty");
  }
  try {
    const value = BigInt(text);
    const maxU64 = (1n << 64n) - 1n;
    if (value < 0n || value > maxU64) {
      throw new Error("out of range");
    }
    return `0x${value.toString(16)}`;
  } catch {
    throw new Error("`--seed` must be a valid u64 (decimal or 0x-prefixed hex)");
  }
}

function parseBool(raw, flagName) {
  const value = String(raw ?? "")
    .trim()
    .toLowerCase();
  if (["1", "true", "yes", "on"].includes(value)) return true;
  if (["0", "false", "no", "off"].includes(value)) return false;
  throw new Error(`\`${flagName}\` must be a boolean (true/false)`);
}

function parseArgs(argv) {
  const out = {
    count: DEFAULT_COUNT,
    valueSizes: DEFAULT_VALUE_SIZES,
    profile: DEFAULT_PROFILE,
    seed: DEFAULT_SEED,
    cacheMb: DEFAULT_CACHE_MB,
    pinInternalPages: DEFAULT_PIN_INTERNAL_PAGES,
    includeColdRead: false,
    json: false,
    progress: false,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--") {
      continue;
    }

    if (arg === "--count") {
      const next = Number(argv[i + 1]);
      if (!Number.isFinite(next) || next <= 0) {
        throw new Error("`--count` must be a positive integer");
      }
      out.count = Math.floor(next);
      i += 1;
      continue;
    }

    if (arg === "--value-sizes") {
      const raw = argv[i + 1] || "";
      const parsed = raw
        .split(",")
        .map((x) => Number(x.trim()))
        .filter((x) => Number.isFinite(x) && x > 0)
        .map((x) => Math.floor(x));

      if (parsed.length === 0) {
        throw new Error("`--value-sizes` must contain positive integers");
      }

      out.valueSizes = parsed;
      i += 1;
      continue;
    }

    if (arg === "--json") {
      out.json = true;
      continue;
    }

    if (arg === "--progress") {
      out.progress = true;
      continue;
    }

    if (arg === "--profile") {
      const next = String(argv[i + 1] || "").trim();
      if (!["basic", "mixed", "range", "all"].includes(next)) {
        throw new Error("`--profile` must be one of: basic, mixed, range, all");
      }
      out.profile = next;
      i += 1;
      continue;
    }

    if (arg === "--seed") {
      const next = argv[i + 1];
      if (next == null) {
        throw new Error("`--seed` requires a value");
      }
      out.seed = parseSeed(next);
      i += 1;
      continue;
    }

    if (arg === "--cache-mb") {
      const next = Number(argv[i + 1]);
      if (!Number.isFinite(next) || next <= 0) {
        throw new Error("`--cache-mb` must be a positive number");
      }
      out.cacheMb = next;
      i += 1;
      continue;
    }

    if (arg === "--pin-internal-pages") {
      out.pinInternalPages = parseBool(argv[i + 1], "--pin-internal-pages");
      i += 1;
      continue;
    }

    if (arg === "--include-cold-read") {
      out.includeColdRead = true;
      continue;
    }

    throw new Error(`Unknown argument: ${arg}`);
  }

  return out;
}

function ensureBuiltPkg(pkgDir) {
  const jsEntry = path.join(pkgDir, "opfs_btree.js");
  const wasmEntry = path.join(pkgDir, "opfs_btree_bg.wasm");
  if (!fs.existsSync(jsEntry) || !fs.existsSync(wasmEntry)) {
    throw new Error(
      "Missing wasm package output. Run `pnpm --dir crates/opfs-btree run bench:wasm:build` first.",
    );
  }
}

function contentType(filePath) {
  if (filePath.endsWith(".js")) return "text/javascript";
  if (filePath.endsWith(".wasm")) return "application/wasm";
  if (filePath.endsWith(".json")) return "application/json";
  if (filePath.endsWith(".html")) return "text/html";
  return "application/octet-stream";
}

function printTable(results) {
  const preferredHeaders = [
    "operation",
    "value_size",
    "count",
    "elapsed_ms",
    "ops_per_sec",
    "p95_op_ms",
    "reads",
    "read_hits",
    "read_misses",
    "writes",
    "deletes",
  ];
  const headers = preferredHeaders.filter((h) =>
    results.some((r) => r[h] !== undefined && r[h] !== null),
  );

  const rows = results.map((r) =>
    headers.map((h) => {
      const v = r[h];
      if (v === undefined || v === null) return "";
      if (h === "ops_per_sec") return Number(v).toFixed(2);
      if (h === "elapsed_ms") return Number(v).toFixed(3);
      if (h === "p95_op_ms") return Number(v).toFixed(4);
      return String(v);
    }),
  );

  const widths = headers.map((h, idx) => Math.max(h.length, ...rows.map((row) => row[idx].length)));
  const line = widths.map((w) => "-".repeat(w)).join("  ");
  const fmt = (row) => row.map((v, idx) => v.padEnd(widths[idx])).join("  ");

  console.log(fmt(headers));
  console.log(line);
  for (const row of rows) {
    console.log(fmt(row));
  }
}

function createWorkerScript() {
  return `
import init, {
  bench_opfs_sequential_write,
  bench_opfs_random_write,
  bench_opfs_sequential_read,
  bench_opfs_random_read,
  bench_opfs_cold_sequential_read,
  bench_opfs_cold_random_read,
  bench_opfs_mixed_scenario,
  bench_opfs_range_seq_window,
  bench_opfs_range_random_window,
  bench_set_cache_bytes,
  bench_set_pin_internal_pages
} from "/pkg/opfs_btree.js";

const pendingRequests = [];
let wasmReady = false;
let initError = null;

const basicRuns = [
  ["seq_write", bench_opfs_sequential_write],
  ["random_write", bench_opfs_random_write],
  ["seq_read", bench_opfs_sequential_read],
  ["random_read", bench_opfs_random_read]
];
const coldRuns = [
  ["cold_seq_read", bench_opfs_cold_sequential_read],
  ["cold_random_read", bench_opfs_cold_random_read]
];
const mixedScenarios = [
  "mixed_random_70r_30w",
  "mixed_random_50r_50w_with_updates",
  "mixed_random_60r_20w_20d"
];
const rangeRuns = [
  ["range_seq_window_64", bench_opfs_range_seq_window],
  ["range_random_window_64", bench_opfs_range_random_window]
];

async function runRequest(payload) {
  const count = Number(payload?.count ?? 5000);
  const valueSizes = Array.isArray(payload?.valueSizes) ? payload.valueSizes : [32, 256, 4096];
  const profile = String(payload?.profile ?? "basic");
  const seedRaw = String(payload?.seed ?? "${DEFAULT_SEED}");
  const includeColdRead = Boolean(payload?.includeColdRead ?? false);
  const cacheMb = Number(payload?.cacheMb ?? ${DEFAULT_CACHE_MB});
  const pinInternalPages = Boolean(payload?.pinInternalPages ?? ${DEFAULT_PIN_INTERNAL_PAGES ? "true" : "false"});
  const seed = BigInt(seedRaw);
  const cacheBytes = Math.max(1, Math.round(cacheMb * 1024 * 1024));
  await bench_set_cache_bytes(cacheBytes);
  bench_set_pin_internal_pages(pinInternalPages);

  try {
    const out = [];
    for (const valueSize of valueSizes) {
      if (profile === "basic" || profile === "all") {
        for (const [name, fn] of basicRuns) {
          const startedAt = performance.now();
          self.postMessage({ type: "progress", event: "start", operation: name, value_size: valueSize });
          const result = await fn(count, valueSize);
          const withName = { ...result, operation: result.operation || name };
          out.push(withName);
          self.postMessage({
            type: "progress",
            event: "end",
            operation: withName.operation,
            value_size: valueSize,
            elapsed_ms: performance.now() - startedAt,
            phase_times_ms: withName.phase_times_ms || []
          });
          self.postMessage({ type: "result", result: withName });
        }
      }

      if (profile === "mixed" || profile === "all") {
        for (const scenario of mixedScenarios) {
          const startedAt = performance.now();
          self.postMessage({ type: "progress", event: "start", operation: scenario, value_size: valueSize });
          const result = await bench_opfs_mixed_scenario(scenario, count, valueSize, seed);
          out.push(result);
          self.postMessage({
            type: "progress",
            event: "end",
            operation: scenario,
            value_size: valueSize,
            elapsed_ms: performance.now() - startedAt,
            phase_times_ms: result.phase_times_ms || []
          });
          self.postMessage({ type: "result", result });
        }
      }

      if (profile === "range" || profile === "all") {
        for (const [name, fn] of rangeRuns) {
          const startedAt = performance.now();
          self.postMessage({ type: "progress", event: "start", operation: name, value_size: valueSize });
          const result = await fn(count, valueSize);
          const withName = { ...result, operation: result.operation || name };
          out.push(withName);
          self.postMessage({
            type: "progress",
            event: "end",
            operation: withName.operation,
            value_size: valueSize,
            elapsed_ms: performance.now() - startedAt,
            phase_times_ms: withName.phase_times_ms || []
          });
          self.postMessage({ type: "result", result: withName });
        }
      }

      if (includeColdRead) {
        for (const [name, fn] of coldRuns) {
          const startedAt = performance.now();
          self.postMessage({ type: "progress", event: "start", operation: name, value_size: valueSize });
          const result = await fn(count, valueSize);
          const withName = { ...result, operation: result.operation || name };
          out.push(withName);
          self.postMessage({
            type: "progress",
            event: "end",
            operation: withName.operation,
            value_size: valueSize,
            elapsed_ms: performance.now() - startedAt,
            phase_times_ms: withName.phase_times_ms || []
          });
          self.postMessage({ type: "result", result: withName });
        }
      }
    }

    self.postMessage({ type: "done", results: out });
  } catch (error) {
    self.postMessage({ type: "error", error: error?.message || String(error) });
  }
}

self.onmessage = (e) => {
  const payload = e.data || {};
  if (initError) {
    self.postMessage({ type: "error", error: initError });
    return;
  }
  if (!wasmReady) {
    pendingRequests.push(payload);
    return;
  }
  void runRequest(payload);
};

(async () => {
  self.postMessage({ type: "progress", event: "worker_boot" });
  try {
    await init();
    wasmReady = true;
    self.postMessage({ type: "progress", event: "wasm_init_done" });
    while (pendingRequests.length > 0) {
      const next = pendingRequests.shift();
      await runRequest(next);
    }
  } catch (error) {
    initError = error?.message || String(error);
    self.postMessage({ type: "error", error: initError });
  }
})();
`;
}

function createHtml(
  count,
  valueSizes,
  profile,
  seed,
  cacheMb,
  pinInternalPages,
  progress,
  includeColdRead,
) {
  return `<!doctype html>
<meta charset="utf-8">
<title>opfs-btree wasm opfs bench</title>
<script>
window.__benchDone = false;
window.__benchError = null;
window.__benchResults = [];
window.__benchProgress = [];
const __emitProgress = ${progress ? "true" : "false"};

const worker = new Worker("/worker.js", { type: "module" });
if (__emitProgress) {
  console.log("[bench-progress]", "page_loaded");
}
worker.onmessage = (e) => {
  const msg = e.data || {};
  if (msg.type === "result") {
    window.__benchResults.push(msg.result);
    if (__emitProgress) {
      console.log("[bench-progress]", JSON.stringify({ type: "result", operation: msg.result?.operation, value_size: msg.result?.value_size }));
    }
  }
  if (msg.type === "progress") {
    window.__benchProgress.push(msg);
    if (__emitProgress) {
      console.log("[bench-progress]", JSON.stringify(msg));
    }
  }
  if (msg.type === "done") {
    window.__benchDone = true;
    if (__emitProgress) {
      console.log("[bench-progress]", "worker_done");
    }
  }
  if (msg.type === "error") {
    window.__benchDone = true;
    window.__benchError = msg.error || "unknown worker error";
    if (__emitProgress) {
      console.log("[bench-progress]", JSON.stringify({ type: "error", error: window.__benchError }));
    }
  }
};
worker.onerror = (e) => {
  window.__benchDone = true;
  window.__benchError = e.message || "worker error";
  if (__emitProgress) {
    console.log("[bench-progress]", JSON.stringify({ type: "worker_onerror", error: window.__benchError }));
  }
};
worker.postMessage({
  count: ${count},
  valueSizes: [${valueSizes.join(",")}],
  profile: "${profile}",
  seed: "${seed}",
  cacheMb: ${cacheMb},
  pinInternalPages: ${pinInternalPages ? "true" : "false"},
  includeColdRead: ${includeColdRead ? "true" : "false"}
});
</script>`;
}

async function run() {
  const args = parseArgs(process.argv.slice(2));
  const benchDir = __dirname;
  const pkgDir = path.join(benchDir, "pkg");
  ensureBuiltPkg(pkgDir);

  const workerScript = createWorkerScript();
  const html = createHtml(
    args.count,
    args.valueSizes,
    args.profile,
    args.seed,
    args.cacheMb,
    args.pinInternalPages,
    args.progress,
    args.includeColdRead,
  );

  const server = http.createServer((req, res) => {
    const url = req.url || "/";

    if (url === "/") {
      res.writeHead(200, { "Content-Type": "text/html" });
      res.end(html);
      return;
    }

    if (url === "/worker.js") {
      res.writeHead(200, { "Content-Type": "text/javascript" });
      res.end(workerScript);
      return;
    }

    if (url.startsWith("/pkg/")) {
      const rel = url.slice("/pkg/".length);
      const filePath = path.join(pkgDir, rel);
      if (!filePath.startsWith(pkgDir)) {
        res.writeHead(403);
        res.end("forbidden");
        return;
      }
      if (!fs.existsSync(filePath) || !fs.statSync(filePath).isFile()) {
        res.writeHead(404);
        res.end("not found");
        return;
      }
      res.writeHead(200, { "Content-Type": contentType(filePath) });
      fs.createReadStream(filePath).pipe(res);
      return;
    }

    res.writeHead(404);
    res.end("not found");
  });

  let browser;
  let context;

  try {
    const results = await new Promise((resolve, reject) => {
      server.listen(0, "127.0.0.1", async () => {
        try {
          const port = server.address().port;
          const baseUrl = `http://127.0.0.1:${port}`;

          browser = await chromium.launch({ headless: true });
          context = await browser.newContext();
          const page = await context.newPage();
          if (args.progress) {
            page.on("console", (msg) => {
              console.log(msg.text());
            });
          }
          await page.goto(baseUrl, { waitUntil: "load", timeout: 60_000 });

          await page.waitForFunction(() => window.__benchDone === true, undefined, {
            timeout: 30 * 60 * 1000,
          });

          const done = await page.evaluate(() => ({
            error: window.__benchError,
            results: window.__benchResults,
          }));

          if (done.error) {
            reject(new Error(done.error));
            return;
          }

          resolve(done.results);
        } catch (error) {
          reject(error);
        }
      });
    });

    if (!Array.isArray(results) || results.length === 0) {
      throw new Error("No benchmark results collected");
    }

    if (args.json) {
      console.log(JSON.stringify(results, null, 2));
    } else {
      printTable(results);
    }
  } finally {
    if (context) {
      await context.close();
    }
    if (browser) {
      await browser.close();
    }
    await new Promise((resolve) => server.close(resolve));
  }
}

run().catch((error) => {
  console.error(`wasm bench failed: ${error.message || error}`);
  process.exit(1);
});
