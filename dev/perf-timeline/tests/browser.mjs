import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { chromium } from "playwright";

// Exercise the built app, with deterministic source responses. No private
// token or live upstream service is required for these correctness assertions.
const server = spawn(
  process.execPath,
  ["node_modules/next/dist/bin/next", "start", "--port", "0", "--hostname", "127.0.0.1"],
  {
    cwd: new URL("../", import.meta.url),
    stdio: ["ignore", "pipe", "pipe"],
  },
);
let browser;
let output = "";
server.stdout.on("data", (chunk) => {
  output += chunk;
});
server.stderr.on("data", (chunk) => {
  output += chunk;
});
try {
  const origin = await new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      clearInterval(poll);
      reject(new Error(`Server did not start: ${output}`));
    }, 30000);
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
  const point = (stage, i, series) => ({
    stage,
    series,
    runId: `run-${i}`,
    resultId: `receipt-${i}`,
    date: `2026-09-1${i}T12:00:00Z`,
    sha: String(i).repeat(40),
    title: `Checkpoint ${i}`,
    branch: stage === "main" ? "main" : "trial",
    pr: stage === "open" ? 100 : null,
    prStatus: stage === "open" ? "OPEN" : null,
    release: stage === "released" ? "v2.0.0" : null,
    runStatus: "COMPLETED",
    min: 0.9 / i,
    median: 1 / i,
    max: 1.1 / i,
  });
  const fixture = {
    fetchedAt: "2026-09-13T12:00:00Z",
    runCount: 4,
    excludedRuns: 0,
    excludedResults: 0,
    warnings: [],
    releases: [
      { name: "v2.0.0", sha: "1".repeat(40), url: "https://github.com/garden-co/jazz/tree/v2.0.0" },
    ],
    benchmarks: [
      {
        id: "first",
        name: "first_sync_27518_rocksdb",
        points: [
          point("released", 1, "main"),
          point("main", 2, "main"),
          point("open", 3, "pr:100"),
          point("open", 4, "pr:100"),
        ],
      },
      { id: "second", name: "other_benchmark", points: [point("main", 2, "main")] },
    ],
  };
  browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  let fail = false;
  await page.route("**/api/timeline", (route) =>
    route.fulfill({
      status: fail ? 502 : 200,
      contentType: "application/json",
      body: JSON.stringify(fail ? { error: "unavailable" } : fixture),
    }),
  );
  await page.goto(origin);
  await page.getByText("Wallclock timeline", { exact: true }).waitFor();
  async function assertMatchingPreview() {
    const positions = await page.evaluate(() => ({
      large: [...document.querySelectorAll(".chart-point")].map((p) => [
        (Number(p.getAttribute("cx")) - 78) / 992,
        (Number(p.getAttribute("cy")) - 25) / 265,
      ]),
      small: [...document.querySelectorAll(".bench-item.active .sparkline circle")].map((p) => [
        (Number(p.getAttribute("cx")) - 3) / 64,
        (Number(p.getAttribute("cy")) - 3) / 18,
      ]),
    }));
    assert.equal(positions.small.length, positions.large.length);
    positions.large.forEach((p, i) =>
      p.forEach((v, j) => assert.ok(Math.abs(v - positions.small[i][j]) < 0.000001)),
    );
  }
  await assertMatchingPreview();
  assert.match(await page.locator(".benchmark-description").innerText(), /Member, not anonymous/);
  assert.match(await page.getByLabel("Throughput receipt").innerText(), /110,072 visible rows\/s/);
  assert.match(
    await page.getByLabel("Throughput receipt").innerText(),
    /550,360 visible rows\/s\*/,
  );
  await page.getByLabel("Timing display").selectOption("estimated");
  assert.equal(await page.locator(".metrics strong").first().innerText(), "50 ms*");
  assert.match(await page.locator(".chart").textContent(), /ESTIMATED WALLCLOCK\*/);
  assert.match(await page.locator("#estimate-footnote").innerText(), /divided|÷ 5/);
  await assertMatchingPreview();
  await page.getByLabel("Timing display").selectOption("measured");
  assert.equal(await page.locator(".chart-point").count(), 4);
  assert.match(await page.locator(".chart").textContent(), /2026-09-11/);
  assert.match(await page.locator(".chart").textContent(), /2026-09-14/);
  assert.match(await page.locator(".chart").textContent(), /RUN DAY \(UTC\)/);
  assert.equal(await page.locator(".legend .stage").count(), 3);
  assert.equal(await page.getByRole("option", { name: "Past PR trial" }).count(), 0);
  assert.equal(await page.getByRole("option", { name: "Other branch" }).count(), 0);
  await page.getByLabel("Checkpoint status").selectOption("open");
  assert.equal(await page.locator(".chart-point").count(), 2);
  await assertMatchingPreview();
  assert.equal(await page.locator(".chart line[stroke-dasharray='7 5']").count(), 1);
  await page.getByLabel("Checkpoint status").selectOption("released");
  assert.equal(await page.locator(".chart-point").count(), 1);
  await page.getByLabel("Checkpoint status").selectOption("all");
  await page.getByLabel("Log scale").check();
  await page.getByLabel("Min–max").check();
  await assertMatchingPreview();
  assert.equal(await page.locator("svg [cy='NaN']").count(), 0);
  await page.locator(".chart-point").first().focus();
  await page.keyboard.press("Enter");
  assert.match(await page.locator(".receipt").innerText(), /Checkpoint 1/);
  await page.locator(".receipts-table summary").click();
  assert.equal(await page.locator("tbody tr").count(), 4);
  assert.match(await page.locator("tbody").innerText(), /1\.000000000 s/);
  await page.getByLabel("Find a benchmark").fill("other");
  await page.locator(".bench-item").click();
  assert.match(page.url(), /benchmark=second/);
  assert.equal(await page.locator(".chart-point").count(), 1);
  await page.getByLabel("Checkpoint status").selectOption("released");
  assert.match(await page.locator(".benchmark-description").innerText(), /No reviewed description/);
  assert.equal(await page.getByLabel("Throughput receipt").count(), 0);
  assert.equal(await page.locator(".empty-chart").count(), 1);
  fail = true;
  await page.getByRole("button", { name: "Refresh data" }).click();
  await page.locator(".notice[role=alert]").waitFor();
  assert.match(await page.locator(".notice[role=alert]").innerText(), /previously loaded data/);
  fail = false;
  await page.getByRole("button", { name: "Retry", exact: true }).click();
  await page.locator(".notice[role=alert]").waitFor({ state: "hidden" });
  await page.setViewportSize({ width: 390, height: 844 });
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
  await page.goto(`${origin}/?benchmark=second`);
  await page.getByText("Wallclock timeline", { exact: true }).waitFor();
  assert.equal(await page.locator(".benchmark-heading code").innerText(), "other_benchmark");
  fixture.benchmarks[0].points = Array.from({ length: 52 }, (_, i) => ({
    ...point("main", i + 1, "main"),
    date: new Date(Date.UTC(2026, 8, 13, 0, i)).toISOString(),
    min: i === 0 ? 100 : 18,
    median: i === 0 ? 110 : 19,
    max: i === 0 ? 120 : 20,
  }));
  await page.goto(`${origin}/?benchmark=first`);
  await page.getByText("Wallclock timeline", { exact: true }).waitFor();
  assert.equal(await page.locator(".chart-point").count(), 52);
  await assertMatchingPreview();
  assert.deepEqual(errors, []);
  console.log(
    "Browser receipt: classification, dashed PR trace, filters, exact table, keyboard selection, error/retry, deep links and mobile overflow passed.",
  );
} finally {
  await browser?.close();
  if (server.exitCode === null) {
    server.kill("SIGTERM");
    await once(server, "exit");
  }
}
