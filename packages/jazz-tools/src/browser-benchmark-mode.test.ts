import { describe, expect, it } from "vitest";
import { shouldExcludeRealisticBrowserBench } from "./browser-benchmark-mode.js";

describe("shouldExcludeRealisticBrowserBench", () => {
  it("keeps realistic browser benchmarks out of ordinary browser test runs", () => {
    expect(
      shouldExcludeRealisticBrowserBench({
        argv: ["node", "vitest", "run", "--config", "vitest.config.browser.ts"],
        lifecycleEvent: "test:browser",
      }),
    ).toBe(true);
  });

  it("includes realistic browser benchmarks for the dedicated benchmark script", () => {
    expect(
      shouldExcludeRealisticBrowserBench({
        argv: [
          "node",
          "vitest",
          "run",
          "--config",
          "vitest.config.browser.ts",
          "tests/browser/realistic-bench.test.ts",
        ],
        lifecycleEvent: "bench:realistic:browser",
      }),
    ).toBe(false);
  });

  it("includes realistic browser benchmarks when the file is targeted explicitly", () => {
    expect(
      shouldExcludeRealisticBrowserBench({
        argv: [
          "node",
          "vitest",
          "run",
          "--config",
          "vitest.config.browser.ts",
          "tests/browser/realistic-bench.test.ts",
        ],
        lifecycleEvent: "",
      }),
    ).toBe(false);
  });
});
