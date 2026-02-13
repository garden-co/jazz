#!/usr/bin/env node

const fs = require("fs");
const http = require("http");
const path = require("path");
const { chromium } = require("playwright");

const DEFAULT_COUNT = 5000;
const DEFAULT_VALUE_SIZES = [32, 256, 4096];
const DEFAULT_PROFILE = "basic";

function parseArgs(argv) {
  const out = {
    count: DEFAULT_COUNT,
    valueSizes: DEFAULT_VALUE_SIZES,
    profile: DEFAULT_PROFILE,
    json: false,
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

    if (arg === "--profile") {
      const next = String(argv[i + 1] || "").trim();
      if (!["basic", "mixed", "all"].includes(next)) {
        throw new Error("`--profile` must be one of: basic, mixed, all");
      }
      out.profile = next;
      i += 1;
      continue;
    }

    throw new Error(`Unknown argument: ${arg}`);
  }

  return out;
}

function ensureBuiltPkg(pkgDir) {
  const jsEntry = path.join(pkgDir, "jazz_lsm.js");
  const wasmEntry = path.join(pkgDir, "jazz_lsm_bg.wasm");
  if (!fs.existsSync(jsEntry) || !fs.existsSync(wasmEntry)) {
    throw new Error(
      "Missing wasm package output. Run `pnpm --dir crates/jazz-lsm run bench:wasm:build` first."
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
    results.some((r) => r[h] !== undefined && r[h] !== null)
  );

  const rows = results.map((r) =>
    headers.map((h) => {
      const v = r[h];
      if (v === undefined || v === null) return "";
      if (h === "ops_per_sec") return Number(v).toFixed(2);
      if (h === "elapsed_ms") return Number(v).toFixed(3);
      if (h === "p95_op_ms") return Number(v).toFixed(4);
      return String(v);
    })
  );

  const widths = headers.map((h, idx) =>
    Math.max(h.length, ...rows.map((row) => row[idx].length))
  );
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
  bench_opfs_mixed_scenario
} from "/pkg/jazz_lsm.js";

await init();

self.onmessage = async (e) => {
  const count = Number(e.data?.count ?? 5000);
  const valueSizes = Array.isArray(e.data?.valueSizes) ? e.data.valueSizes : [32, 256, 4096];
  const profile = String(e.data?.profile ?? "basic");

  const basicRuns = [
    ["seq_write", bench_opfs_sequential_write],
    ["random_write", bench_opfs_random_write],
    ["seq_read", bench_opfs_sequential_read],
    ["random_read", bench_opfs_random_read]
  ];
  const mixedScenarios = [
    "mixed_random_70r_30w",
    "mixed_random_50r_50w_with_updates",
    "mixed_random_60r_20w_20d"
  ];

  try {
    const out = [];
    for (const valueSize of valueSizes) {
      if (profile === "basic" || profile === "all") {
        for (const [name, fn] of basicRuns) {
          const result = await fn(count, valueSize);
          const withName = { ...result, operation: result.operation || name };
          out.push(withName);
          self.postMessage({ type: "result", result: withName });
        }
      }

      if (profile === "mixed" || profile === "all") {
        for (const scenario of mixedScenarios) {
          const result = await bench_opfs_mixed_scenario(scenario, count, valueSize);
          out.push(result);
          self.postMessage({ type: "result", result });
        }
      }
    }

    self.postMessage({ type: "done", results: out });
  } catch (error) {
    self.postMessage({ type: "error", error: error?.message || String(error) });
  }
};
`;
}

function createHtml(count, valueSizes, profile) {
  return `<!doctype html>
<meta charset="utf-8">
<title>jazz-lsm wasm opfs bench</title>
<script>
window.__benchDone = false;
window.__benchError = null;
window.__benchResults = [];

const worker = new Worker("/worker.js", { type: "module" });
worker.onmessage = (e) => {
  const msg = e.data || {};
  if (msg.type === "result") {
    window.__benchResults.push(msg.result);
  }
  if (msg.type === "done") {
    window.__benchDone = true;
  }
  if (msg.type === "error") {
    window.__benchDone = true;
    window.__benchError = msg.error || "unknown worker error";
  }
};
worker.onerror = (e) => {
  window.__benchDone = true;
  window.__benchError = e.message || "worker error";
};
worker.postMessage({ count: ${count}, valueSizes: [${valueSizes.join(",")}], profile: "${profile}" });
</script>`;
}

async function run() {
  const args = parseArgs(process.argv.slice(2));
  const benchDir = __dirname;
  const pkgDir = path.join(benchDir, "pkg");
  ensureBuiltPkg(pkgDir);

  const workerScript = createWorkerScript();
  const html = createHtml(args.count, args.valueSizes, args.profile);

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
          await page.goto(baseUrl, { waitUntil: "load", timeout: 60_000 });

          await page.waitForFunction(
            () => window.__benchDone === true,
            undefined,
            { timeout: 30 * 60 * 1000 }
          );

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
