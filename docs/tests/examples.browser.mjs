import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { chromium } from "playwright";
import { formatTime } from "../lib/perf-timeline/model.ts";
import { heroExamples, moreBenchmarkSections } from "../lib/showcase/catalogue.ts";

// Follow the catalogue rather than hardcoding one example: the first hero and
// its first per-operation metric card, and two engine rows under "More
// benchmarks" that share a name but not a scenario.
const hero = heroExamples.find((example) => example.metrics.some((metric) => metric.per));
const metric = hero.metrics.find((candidate) => candidate.per);
const perCount = metric.per.count;
const estimate = (seconds) => `${formatTime(seconds / 5)}*`;
const perOperation = (seconds) => estimate(seconds / perCount);
const engine = moreBenchmarkSections.find((section) => section.benchmarks);
const [engineA, engineB] = engine.benchmarks.filter(
  (row, _, rows) => rows.filter((other) => other.name === row.name).length > 1,
);

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
        name: metric.benchmark,
        points: [
          point("released", 1, 4, { includedInRelease: "v2.0.0-alpha.1" }),
          point("released", 2, 2, { release: "v2.0.0-alpha.2" }),
          point("main", 3, 1),
          point("open", 4, 0.001),
        ],
      },
      { id: engineA.id, name: engineA.name, points: [point("main", 2, 0.5)] },
      { id: engineB.id, name: engineB.name, points: [point("main", 2, 0.75)] },
      // A size CodSpeed no longer measures on every merge is not listed.
      { id: "retired-size", name: "prepared_cold[500]", points: [point("main", 2, 0.125)] },
      // A retired name still in CodSpeed history is not listed.
      { id: "retired", name: "retired_benchmark", points: [point("main", 2, 0.25)] },
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
  // The top nav lays out its links after hydration.
  await page.locator('a[href="/examples"]').first().waitFor({ state: "attached" });

  // Newest released number, per operation, with the /5 estimate: 2 s / 5 / count.
  const card = page.locator(`#${hero.id} .metric-card`).first();
  await card
    .locator(".metric-headline")
    .filter({ hasText: perOperation(2) })
    .waitFor();
  assert.ok((await card.innerText()).includes(`per ${metric.per.unit}`));
  assert.match(await card.innerText(), /Released in v2\.0\.0-alpha\.2/);
  assert.match(await card.innerText(), /−50%/);

  // History card: one row per release plus the unreleased main number.
  const tooltip = page.getByRole("dialog", { name: `History of ${metric.label}` });
  assert.equal(await tooltip.isVisible(), false);
  await card.hover();
  await tooltip.waitFor({ state: "visible" });
  const history = await tooltip.innerText();
  assert.match(history, /v2\.0\.0-alpha\.1/);
  assert.ok(history.includes(perOperation(4)));
  assert.match(history, /Unreleased main/);
  assert.ok(history.includes(`Measured on the CodSpeed runner: ${formatTime(2 / perCount)}`));
  // Open-PR experiments never feed a card or its history.
  assert.ok(!`${await card.innerText()} ${history}`.includes(formatTime(0.001 / 5 / perCount)));

  // Engine results are listed under More benchmarks by id, one row per
  // scenario, attributed to main when unreleased; retired names and sizes are
  // not.
  const more = await page.locator("#benchmarks").innerText();
  const engineSection = await page.locator(`#${engine.id}`).innerText();
  for (const [row, median] of [
    [engineA, 0.5],
    [engineB, 0.75],
  ]) {
    assert.ok(engineSection.includes(row.scenario), row.uri);
    assert.ok(engineSection.includes(estimate(median)), row.uri);
  }
  assert.doesNotMatch(more, /retired_benchmark|prepared_cold\[500\]/);
  assert.equal(await page.locator(`#${hero.id} video`).count(), hero.video ? 1 : 0);

  await page.setViewportSize({ width: 390, height: 844 });
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);

  // A source outage is visible, not silently empty.
  fail = true;
  await page.reload();
  await page
    .getByRole("alert")
    .filter({ hasText: "Benchmark history is temporarily unavailable" })
    .waitFor();

  assert.deepEqual(errors, []);
  console.log(
    "Examples page receipt: redirect, released per-operation card, estimate, history popover, open-PR exclusion, More benchmarks sections, mobile overflow and outage notice passed.",
  );
} finally {
  await browser?.close();
  if (server.exitCode === null) {
    server.kill("SIGTERM");
    await once(server, "exit");
  }
}
