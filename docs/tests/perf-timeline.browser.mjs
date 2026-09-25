import assert from "node:assert/strict";
import { mkdir } from "node:fs/promises";
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
  const page = await browser.newPage({
    viewport: { width: 1440, height: 1000 },
    timezoneId: "UTC",
  });
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
  await page.goto(`${origin}/perf-timeline`);
  await page.getByText("Wallclock timeline", { exact: true }).waitFor();
  assert.equal(await page.getByLabel("Log scale").isChecked(), true);
  assert.match(await page.locator(".chart").textContent(), /LOG SCALE/);
  await page.getByLabel("Log scale").uncheck();
  assert.match(await page.locator(".chart").textContent(), /ZERO-BASED SCALE/);
  async function assertMatchingPreview() {
    const positions = await page.evaluate(() => ({
      large: [...document.querySelectorAll(".chart-point")].map((p) => [
        (Number(p.getAttribute("cx")) - 78) / 944,
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
  // The old route redirects into the examples & benchmarks page, which hosts the explorer.
  assert.equal(new URL(page.url()).pathname, "/examples");
  assert.equal(await page.locator('nav a[href="/perf-timeline"]').count(), 0);
  assert.ok((await page.locator('a[href="/examples"]').count()) > 0);
  assert.equal(await page.getByRole("link", { name: "Jazz home" }).count(), 1);
  const desktopReceipt = await page.locator(".receipt-body").evaluate((el) => {
    const details = el.firstElementChild.getBoundingClientRect();
    const timing = el.lastElementChild.getBoundingClientRect();
    return {
      detailsRight: details.right,
      timingLeft: timing.left,
      detailsTop: details.top,
      timingTop: timing.top,
    };
  });
  assert.ok(
    desktopReceipt.timingLeft > desktopReceipt.detailsRight,
    "desktop receipt places timing alongside details",
  );
  assert.equal(desktopReceipt.detailsTop, desktopReceipt.timingTop);
  await mkdir(new URL("../.next/perf-timeline-receipts/", import.meta.url), { recursive: true });
  await page.screenshot({
    path: new URL("../.next/perf-timeline-receipts/desktop.png", import.meta.url).pathname,
    fullPage: true,
  });
  await assertMatchingPreview();
  assert.match(await page.locator(".benchmark-description").innerText(), /Member, not anonymous/);
  assert.match(await page.getByLabel("Throughput receipt").innerText(), /110,072 visible rows\/s/);
  assert.match(
    await page.getByLabel("Throughput receipt").innerText(),
    /550,360 visible rows\/s\*/,
  );
  assert.equal(await page.getByLabel("Timing display").inputValue(), "estimated");
  assert.equal(await page.locator(".metrics strong").first().innerText(), "200 ms*");
  assert.equal(await page.locator(".metrics strong").nth(1).innerText(), "50 ms*");
  assert.deepEqual(await page.locator(".metrics > div > span").allTextContents(), [
    "First shown",
    "Latest shown",
    "Measured checkpoints",
  ]);
  assert.match(
    await page.locator(".metrics .metric-throughput").nth(1).innerText(),
    /550,360 visible rows\/s\*/,
  );
  async function assertMirroredTicks() {
    const left = await page.locator(".y-axis-left").allTextContents();
    assert.deepEqual(await page.locator(".y-axis-right").allTextContents(), left);
    return left;
  }
  assert.deepEqual(await assertMirroredTicks(), ["0 s*", "100 ms*", "200 ms*", "300 ms*"]);
  assert.match(await page.locator(".chart").textContent(), /ESTIMATED WALLCLOCK\*/);
  assert.match(await page.locator("#estimate-footnote").innerText(), /divided|÷ 5/);
  await assertMatchingPreview();
  await page.getByLabel("Timing display").selectOption("measured");
  assert.equal(await page.locator(".metrics strong").nth(1).innerText(), "250 ms");
  assert.match(
    await page.locator(".metrics .metric-throughput").nth(1).innerText(),
    /110,072 visible rows\/s/,
  );
  assert.match(await page.locator(".chart").textContent(), /DETERMINISTIC RUNNER/);
  await assertMatchingPreview();
  assert.equal(await page.locator(".chart-point").count(), 4);
  assert.match(await page.locator(".chart").textContent(), /2026-09-11/);
  assert.match(await page.locator(".chart").textContent(), /2026-09-14/);
  assert.match(await page.locator(".chart").textContent(), /CHECKPOINT DAY \(UTC\)/);
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
  assert.deepEqual(await assertMirroredTicks(), ["100 ms", "200 ms", "500 ms", "1 s", "2 s"]);
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
  await page.goto(`${origin}/perf-timeline?benchmark=second`);
  await page.getByText("Wallclock timeline", { exact: true }).waitFor();
  assert.equal(await page.locator(".benchmark-heading code").innerText(), "other_benchmark");
  fixture.benchmarks[0].points = Array.from({ length: 52 }, (_, i) => ({
    ...point("main", i + 1, "main"),
    date: new Date(Date.UTC(2026, 8, 13, 0, i)).toISOString(),
    min: i === 0 ? 100 : 18,
    median: i === 0 ? 110 : 19,
    max: i === 0 ? 120 : 20,
  }));
  await page.goto(`${origin}/perf-timeline?benchmark=first`);
  await page.getByText("Wallclock timeline", { exact: true }).waitFor();
  assert.equal(await page.locator(".chart-point").count(), 52);
  await assertMatchingPreview();
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
  const mobileReceipt = await page.locator(".receipt-body").evaluate((el) => {
    const details = el.firstElementChild.getBoundingClientRect();
    const timing = el.lastElementChild.getBoundingClientRect();
    return {
      detailsBottom: details.bottom,
      timingTop: timing.top,
      detailsLeft: details.left,
      timingLeft: timing.left,
    };
  });
  assert.ok(
    mobileReceipt.timingTop > mobileReceipt.detailsBottom,
    "mobile receipt stacks timing below details",
  );
  assert.equal(mobileReceipt.detailsLeft, mobileReceipt.timingLeft);
  await page.screenshot({
    path: new URL("../.next/perf-timeline-receipts/mobile.png", import.meta.url).pathname,
    fullPage: true,
  });
  await page.evaluate(() => document.documentElement.classList.add("dark"));
  const theme = await page.locator(".perf-timeline").evaluate((el) => ({
    foreground: getComputedStyle(el).color,
    background: getComputedStyle(el).backgroundColor,
    font: getComputedStyle(el).fontFamily,
  }));
  assert.notEqual(theme.foreground, theme.background);
  assert.match(theme.font, /body_font/);
  await page.screenshot({
    path: new URL("../.next/perf-timeline-receipts/mobile-dark.png", import.meta.url).pathname,
    fullPage: true,
  });

  // Synthetic browser response only: no fabricated receipt is sent upstream.
  const historical = {
    ...point("released", 1, "main"),
    date: "2026-09-10T04:26:57.178Z",
    measuredAt: "2026-09-14T15:00:00Z",
    release: null,
    includedInRelease: null,
    backfill: {
      releaseTag: "v2.0.0-alpha.54",
      engineSha: "e".repeat(40),
      harnessSha: "1".repeat(40),
      harnessSourceSha: "s".repeat(40),
      effectiveDate: "2026-09-10T04:26:57.178Z",
      dateSource: "synthetic publication fixture",
      workflowUrl: "https://example.com/synthetic-workflow",
      receipts: [],
    },
  };
  fixture.benchmarks[0].points = [historical, point("main", 4, "main")];
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto(`${origin}/perf-timeline?benchmark=first`);
  await page.getByText("Wallclock timeline", { exact: true }).waitFor();
  assert.equal(await page.locator(".chart-point").count(), 2);
  assert.match(await page.locator(".chart").textContent(), /2026-09-10/);
  assert.match(
    await page
      .locator(".chart .axis-text")
      .allTextContents()
      .then((labels) => labels.join(" ")),
    /v2.0.0-alpha.54/,
  );
  assert.equal(await page.locator('.chart line[opacity="0.8"]').count(), 1);
  assert.equal(await page.locator(".bench-item.active .sparkline line").count(), 1);
  await page.screenshot({
    path: new URL("../.next/perf-timeline-receipts/released-main-continuity.png", import.meta.url)
      .pathname,
    fullPage: true,
  });
  assert.equal(await page.getByRole("option", { name: "Historical backfill" }).count(), 0);
  await page.locator(".chart-point").first().focus();
  await page.keyboard.press("Enter");
  assert.match(await page.locator(".receipt").innerText(), /Released/);
  assert.match(await page.locator(".receipt").innerText(), /Release date Sep 10.*measured Sep 14/);
  assert.equal(await page.locator(`.receipt a[href$="/commit/${"1".repeat(40)}"]`).count(), 1);
  assert.equal(await page.locator(`.receipt a[href$="/commit/${"e".repeat(40)}"]`).count(), 1);
  assert.match(await page.locator(".receipt-id").innerText(), /Historical harness/);
  assert.doesNotMatch(await page.locator(".receipt-id").innerText(), /exact release commit/);
  await page.getByLabel("Checkpoint status").selectOption("released");
  assert.equal(await page.locator(".chart-point").count(), 1);
  await page.locator(".receipts-table summary").click();
  assert.match(await page.locator("tbody").innerText(), /v2.0.0-alpha.54/);
  assert.match(await page.locator("tbody").innerText(), /Sep 10.*measured Sep 14/);
  await page.screenshot({
    path: new URL("../.next/perf-timeline-receipts/historical-backfill.png", import.meta.url)
      .pathname,
    fullPage: true,
  });
  await page.setViewportSize({ width: 390, height: 844 });
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
  fixture.benchmarks[0].points = [historical];
  await page.goto(`${origin}/perf-timeline?benchmark=first`);
  await page.getByText("Wallclock timeline", { exact: true }).waitFor();
  assert.equal(
    await page.getByLabel("Branch trace").locator('option[value="main"]').textContent(),
    "main",
  );
  console.log(
    "Historical backfill browser receipt: release placement, measured date, both source commits, Released classification, continuous main trace and mobile layout passed.",
  );

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
