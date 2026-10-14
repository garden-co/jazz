// Registers one app's load-time cases with Vitest bench. Each app has its own
// file in benches/, so a failing app loses only its own results: the
// benchmark runner drops a file's results when any of its cases fails.
//
// Per app: start its servers and the browser, create the fixture users (both
// untimed), then run the cases in metadata order. Each iteration runs the
// scenario's `prepare` (untimed), then times `run`: from the user action until
// the awaited content is visible. `finish` runs untimed after each iteration.
import assert from "node:assert/strict";
import { afterAll, beforeAll, bench } from "vitest";
import { killStale, launchBrowser, startApp } from "./harness.mjs";
import { benchmarkName, loadTimeCases } from "./metadata.ts";

const rounds = Number(process.env.BENCH_ROUNDS || 3);
assert.ok(Number.isInteger(rounds) && rounds > 0, "BENCH_ROUNDS must be a positive integer");
const filter = process.env.BENCH_FILTER || "";

export function defineLoadTimeBenchmarks(appId, load) {
  const cases = loadTimeCases.filter((c) => c.app === appId);
  assert.ok(cases.length, `${appId} has no cases in metadata.ts`);
  let server, browser, app;

  beforeAll(async () => {
    killStale();
    server = await startApp(appId);
    browser = await launchBrowser();
    const { default: create } = await load();
    app = create({ server, browser });
    const keys = Object.keys(app.scenarios).sort();
    assert.deepEqual(
      keys,
      cases.map((c) => c.scenario).sort(),
      `${appId}: scenarios match metadata.ts`,
    );
    try {
      await app.setup?.();
    } catch (e) {
      console.log(server.log());
      throw e;
    }
  }, 600_000);

  afterAll(async () => {
    await app?.close?.().catch(() => {});
    await browser?.close().catch(() => {});
    await server?.stop();
  }, 120_000);

  for (const c of cases) {
    const name = benchmarkName(c);
    if (!name.includes(filter)) continue;
    let ctx;
    bench(
      name,
      async () => {
        await app.scenarios[c.scenario].run(ctx);
      },
      {
        iterations: rounds,
        time: 0,
        warmupIterations: 0,
        warmupTime: 0,
        setup(task) {
          // Vitest passes options to Bench, so install per-sample hooks on its Task.
          const scenario = () => app.scenarios[c.scenario];
          task.opts.beforeEach = async () => {
            ctx = (await scenario().prepare?.()) ?? {};
          };
          task.opts.afterEach = async () => {
            await scenario().finish?.(ctx);
            ctx = undefined;
          };
        },
      },
    );
  }
}
