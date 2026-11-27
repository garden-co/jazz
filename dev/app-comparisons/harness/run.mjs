#!/usr/bin/env node
// Usage: node harness/run.mjs <hero>/<stack> [--variant <name>] [--runs 7] [--samples 20] [--no-seed]
// Imports the target's harness.mjs (see apps.mjs for where each target lives):
// { seed(), start({ variant }), distDir, serverUrl? }.
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { parseArgs } from "node:util";
import { chromium } from "playwright";
import { resolveTarget, resultsDir } from "./apps.mjs";
import { bundleGzip, retries, rtt, SETTLE_MS } from "./lib.mjs";
import { reloadsSeen, watchBrowser } from "./watch.mjs";

const { values, positionals } = parseArgs({
  allowPositionals: true,
  options: {
    variant: { type: "string", default: "default" },
    runs: { type: "string", default: "7" },
    samples: { type: "string", default: "20" },
    "no-seed": { type: "boolean", default: false },
  },
});
const target = positionals[0];
if (!target) throw new Error("usage: run.mjs <hero>/<stack>");
const { hero, stack, appDir, hooksDir } = resolveTarget(target);
const app = await import(pathToFileURL(join(hooksDir, "harness.mjs")).href);
const scenarios = await import(`./scenarios/${hero}.mjs`);
const log = (...a) => console.error(...a);

log(`# ${target} (${values.variant})`);
const ctx = values["no-seed"] ? app.lastSeed?.() : await app.seed({ variant: values.variant });
log("seeded", JSON.stringify(ctx));
const server = await app.start({ variant: values.variant });
log("serving", server.url);
const browser = await chromium.launch();
watchBrowser(browser);
let result;
try {
  result = await scenarios.run({
    browser,
    app,
    url: server.url,
    ctx,
    runs: Number(values.runs),
    samples: Number(values.samples),
    log,
  });
  result.bundle = bundleGzip(join(appDir, app.distDir ?? "dist"));
  result.rtt_ms = await rtt(server.rttUrl ?? server.url);
  result.lost_samples = retries.count;
  result.settle_ms = SETTLE_MS;
  result.self_reloads = reloadsSeen.count;
  result.self_reload_phases = reloadsSeen.phases;
} finally {
  await browser.close();
  await server.stop();
}
const record = {
  hero,
  stack,
  variant: values.variant,
  date: new Date().toISOString(),
  where: server.where ?? "local",
  runs: Number(values.runs),
  samples: Number(values.samples),
  ...result,
};
const day = record.date.slice(0, 10);
mkdirSync(join(resultsDir, day), { recursive: true });
const file = join(resultsDir, day, `${hero}.${stack}.${values.variant}.json`);
writeFileSync(file, JSON.stringify(record, null, 2) + "\n");
console.log(JSON.stringify(record, null, 2));
log(`wrote ${file}`);
