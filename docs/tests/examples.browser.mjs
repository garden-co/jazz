import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { chromium } from "playwright";

// Exercise the built examples page against a deterministic /api/timeline
// fixture. No token or live CodSpeed/GitHub access is needed.
const server = spawn(
  process.execPath,
  ["node_modules/next/dist/bin/next", "start", "--port", "0", "--hostname", "127.0.0.1"],
  { cwd: new URL("../", import.meta.url), stdio: ["ignore", "pipe", "pipe"] },
);
let output = "";
server.stdout.on("data", (chunk) => (output += chunk));
server.stderr.on("data", (chunk) => (output += chunk));
let browser;
try {
  const origin = await new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error(`Server did not start: ${output}`)), 30000);
    const poll = setInterval(() => {
      const match = output.match(/http:\/\/127\.0\.0\.1:\d+/);
      if (match && output.includes("Ready")) {
        clearInterval(poll);
        clearTimeout(timeout);
        resolve(match[0]);
      } else if (server.exitCode !== null) {
        clearInterval(poll);
        clearTimeout(timeout);
        reject(new Error(output));
      }
    }, 50);
  });
  let serial = 0;
  const point = (stage, day, median, extra = {}) => ({
    stage,
    series: stage === "open" ? "pr:100" : "main",
    runId: `run-${++serial}`,
    resultId: `receipt-${serial}`,
    date: `2026-09-1${day}T12:00:00Z`,
    measuredAt: `2026-09-1${day}T12:00:00Z`,
    backfill: null,
    sha: String(day).repeat(40),
    title: `Checkpoint ${day}`,
    branch: stage === "open" ? "trial" : "main",
    pr: stage === "open" ? 100 : null,
    prStatus: stage === "open" ? "OPEN" : null,
    release: null,
    includedInRelease: null,
    runStatus: "COMPLETED",
    min: median,
    median,
    max: median,
    ...extra,
  });
  const fixture = {
    fetchedAt: "2026-09-15T12:00:00Z",
    runCount: 4,
    excludedRuns: 0,
    excludedResults: 0,
    warnings: [],
    releases: [],
    benchmarks: [
      {
        id: "insert",
        name: "sequential_insert_1350_rocksdb",
        points: [
          point("released", 1, 4, { includedInRelease: "v2.0.0-alpha.1" }),
          point("released", 2, 2, { release: "v2.0.0-alpha.2" }),
          point("main", 3, 1),
          point("open", 4, 0.001),
        ],
      },
      { id: "other", name: "other_benchmark", points: [point("main", 2, 0.5)] },
    ],
  };
  let fail = false;
  browser = await chromium.launch({
    headless: true,
    executablePath: process.env.CHROMIUM_PATH || undefined,
  });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.route("**/api/timeline", (route) =>
    route.fulfill({
      status: fail ? 502 : 200,
      contentType: "application/json",
      body: JSON.stringify(fail ? { error: "unavailable" } : fixture),
    }),
  );

  // The old explorer route lands on the examples page.
  await page.goto(`${origin}/perf-timeline?benchmark=insert`);
  assert.equal(new URL(page.url()).pathname, "/examples");
  assert.ok((await page.locator('a[href="/examples"]').count()) > 0);

  // Newest released number, per insert, with the /5 estimate: 2 s / 5 / 1350.
  const card = page.locator("#todo .group").first();
  await card.locator("div.text-2xl").filter({ hasText: "296.3 µs*" }).waitFor();
  assert.match(await card.innerText(), /per insert/);
  assert.match(await card.innerText(), /Released in v2\.0\.0-alpha\.2/);
  assert.match(await card.innerText(), /−50%/);

  // History popover: one row per release plus the unreleased main number.
  const tooltip = card.getByRole("tooltip");
  assert.equal(await tooltip.isVisible(), false);
  await card.hover();
  await tooltip.waitFor({ state: "visible" });
  const history = await tooltip.innerText();
  assert.match(history, /v2\.0\.0-alpha\.1/);
  assert.match(history, /592\.59 µs\*/);
  assert.match(history, /Unreleased main/);
  assert.match(history, /Measured on the CodSpeed runner: 1\.48 ms/);
  // Open-PR experiments never feed a card or its history (0.001 s / 5 / 1350).
  assert.doesNotMatch(`${await card.innerText()} ${history}`, /0\.15 µs/);

  // Every other benchmark is listed, attributed to main when unreleased.
  const misc = await page.locator("#benchmarks").innerText();
  assert.match(misc, /other_benchmark/);
  assert.match(misc, /100 ms\*/);
  assert.equal(await page.locator("#todo video").count(), 1);

  await page.setViewportSize({ width: 390, height: 844 });
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);

  // A source outage is visible, not silently empty.
  fail = true;
  await page.reload();
  await page
    .getByRole("alert")
    .filter({ hasText: "CodSpeed history is temporarily unavailable" })
    .waitFor();

  assert.deepEqual(errors, []);
  console.log(
    "Examples page receipt: redirect, released per-operation card, estimate, history popover, open-PR exclusion, misc list, mobile overflow and outage notice passed.",
  );
} finally {
  await browser?.close();
  if (server.exitCode === null) {
    server.kill("SIGTERM");
    await once(server, "exit");
  }
}
