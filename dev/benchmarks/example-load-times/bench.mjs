// Registers one app's load-time cases with Vitest bench. Each app has its own
// file in benches/, so a failing app loses only its own results: the
// benchmark runner drops a file's results when any of its cases fails.
//
// Per app: start its servers and the browser, create the fixture users (both
// untimed), then run the cases in metadata order. Each iteration runs the
// scenario's `prepare` (untimed), then times `run`: from the user action until
// the awaited content is visible. `finish` runs untimed after each iteration.
import assert from "node:assert/strict";
import { afterAll, beforeAll, test } from "vitest";
import {
  cpuByGroup,
  eventsSince,
  killStale,
  launchBrowser,
  now,
  startApp,
  tracing,
} from "./harness.mjs";
import { benchmarkName, loadTimeCases } from "./metadata.ts";

const rounds = Number(process.env.BENCH_ROUNDS || 3);
assert.ok(Number.isInteger(rounds) && rounds > 0, "BENCH_ROUNDS must be a positive integer");
const filter = process.env.BENCH_FILTER || "";

export function defineLoadTimeBenchmarks(appId, load) {
  const cases = loadTimeCases.filter((c) => c.app === appId);
  assert.ok(cases.length, `${appId} has no cases in metadata.ts`);
  const selected = cases.filter((c) => benchmarkName(c).includes(filter));
  if (!selected.length) return;
  let server, browser, app;

  const trace = (line) => tracing && console.log(`[trace ${new Date().toISOString()}] ${line}`);
  const timed = async (label, fn) => {
    const t0 = now();
    try {
      return await fn();
    } finally {
      trace(`${appId} ${label}: ${Math.round(now() - t0)} ms`);
    }
  };

  beforeAll(async () => {
    killStale();
    server = await timed("start servers", () => startApp(appId));
    browser = await timed("launch browser", () => launchBrowser());
    const { default: create } = await load();
    app = create({ server, browser });
    const keys = Object.keys(app.scenarios).sort();
    assert.deepEqual(
      keys,
      cases.map((c) => c.scenario).sort(),
      `${appId}: scenarios match metadata.ts`,
    );
    try {
      await timed("setup", async () => app.setup?.());
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

  // CodSpeed includes the test name in its benchmark URI. An unnamed group
  // preserves the existing file::scenario identifiers across the Vitest upgrade.
  test("", { timeout: 600_000 * selected.length }, async ({ bench }) => {
    for (const c of selected) {
      const name = benchmarkName(c);
      const scenario = app.scenarios[c.scenario];
      let ctx,
        phase,
        round = 0;
      await bench(
        name,
        {
          async beforeEach() {
            phase = [];
            const t0 = now();
            ctx = (await scenario.prepare?.()) ?? {};
            phase.push(`prepare ${Math.round(now() - t0)} ms`);
          },
          async afterEach() {
            const t0 = now();
            await scenario.finish?.(ctx);
            ctx = undefined;
            phase.push(`finish ${Math.round(now() - t0)} ms`);
            trace(`${name} #${++round}\n    ${phase.join("\n    ")}`);
          },
        },
        async () => {
          if (!tracing) return scenario.run(ctx);
          const t0 = now();
          const cpu0 = cpuByGroup();
          await scenario.run(ctx);
          const ms = now() - t0;
          const cpu1 = cpuByGroup();
          const used = Object.keys(cpu1)
            .map((k) => [k, cpu1[k] - (cpu0[k] ?? 0)])
            .filter(([, v]) => v >= 0.05)
            .map(([k, v]) => `${k}=${v.toFixed(2)}s`)
            .join(" ");
          phase.push(`run ${Math.round(ms)} ms (cpu ${used})`);
          for (const e of eventsSince(t0)) phase.push(`  +${Math.round(e.t - t0)} ms ${e.what}`);
        },
      ).run({ iterations: rounds, time: 0, warmup: false });
    }
  });
}
