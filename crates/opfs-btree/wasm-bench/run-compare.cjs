#!/usr/bin/env node

const fs = require("fs");
const http = require("http");
const path = require("path");

const here = __dirname;
const dist = path.join(here, "dist");

function loadPlaywright() {
  try {
    return require("playwright");
  } catch {
    return require(path.join(here, "..", "node_modules", "playwright"));
  }
}

function parseArgs(argv) {
  const out = { profiles: ["objects", "wikipedia"], json: false };
  for (let i = 0; i < argv.length; i += 1) {
    if (argv[i] === "--") {
      continue;
    } else if (argv[i] === "--profiles") {
      out.profiles = String(argv[++i] || "")
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
    } else if (argv[i] === "--json") {
      out.json = true;
    } else {
      throw new Error(`unknown argument: ${argv[i]}`);
    }
  }
  if (out.profiles.length === 0) throw new Error("--profiles needs at least one profile");
  return out;
}

function contentType(filePath) {
  if (filePath.endsWith(".html")) return "text/html";
  if (filePath.endsWith(".js")) return "text/javascript";
  if (filePath.endsWith(".wasm")) return "application/wasm";
  return "application/octet-stream";
}

function serveFile(res, filePath) {
  const resolved = path.resolve(filePath);
  const distRoot = path.resolve(dist);
  if (
    !resolved.startsWith(`${distRoot}${path.sep}`) ||
    !fs.existsSync(resolved) ||
    !fs.statSync(resolved).isFile()
  ) {
    res.writeHead(404);
    res.end("not found");
    return;
  }
  res.writeHead(200, { "Content-Type": contentType(resolved) });
  fs.createReadStream(resolved).pipe(res);
}

function normalizeResultForNode(value) {
  return JSON.parse(
    JSON.stringify(value, (_key, inner) => (typeof inner === "bigint" ? Number(inner) : inner)),
  );
}

function printTable(result) {
  const rows = [];
  for (const comparison of result.results || []) {
    const sqliteByPhase = new Map(comparison.sqlite.phases.map((phase) => [phase.phase, phase]));
    for (const btreePhase of comparison.btree.phases) {
      const sqlitePhase = sqliteByPhase.get(btreePhase.phase);
      rows.push({
        profile: comparison.profile,
        phase: btreePhase.phase,
        btree_ms: Number(btreePhase.elapsed_ms).toFixed(2),
        sqlite_ms: sqlitePhase ? Number(sqlitePhase.elapsed_ms).toFixed(2) : "",
        btree_ops_s: Math.round(Number(btreePhase.ops_per_sec)),
        sqlite_ops_s: sqlitePhase ? Math.round(Number(sqlitePhase.ops_per_sec)) : "",
      });
    }
  }
  console.log("\n=== in-browser comparison: opfs-btree vs SQLite (Yew + Rust workers, OPFS) ===\n");
  console.table(rows);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const { chromium } = loadPlaywright();
  if (!fs.existsSync(path.join(dist, "index.html"))) {
    throw new Error(`missing harness build output: ${dist}`);
  }

  const server = http.createServer((req, res) => {
    const url = new URL(req.url || "/", "http://127.0.0.1");
    const pathname = url.pathname === "/" ? "/index.html" : url.pathname;
    serveFile(res, path.join(dist, decodeURIComponent(pathname)));
  });

  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = server.address().port;
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage();
    page.on("console", (message) => console.error(`[browser:${message.type()}] ${message.text()}`));
    page.on("pageerror", (error) => console.error("[pageerror]", error.message));
    page.on("requestfailed", (request) => {
      console.error(`[requestfailed] ${request.url()} ${request.failure()?.errorText || ""}`);
    });
    const profiles = encodeURIComponent(args.profiles.join(","));
    await page.goto(`http://127.0.0.1:${port}/?profiles=${profiles}&autorun=1`, {
      waitUntil: "load",
      timeout: 60000,
    });
    await page.waitForFunction(() => window.__benchDone === true, undefined, {
      timeout: 600000,
    });
    const result = normalizeResultForNode(await page.evaluate(() => window.__benchResult));
    if (!result || result.ok !== true) {
      throw new Error(result?.error?.error || "benchmark failed without an error payload");
    }
    if (args.json) {
      console.log(JSON.stringify(result, null, 2));
    } else {
      printTable(result);
    }
  } finally {
    await browser.close();
    await new Promise((resolve) => server.close(resolve));
  }
}

main().catch((error) => {
  console.error("compare failed:", error.message || error);
  process.exit(1);
});
