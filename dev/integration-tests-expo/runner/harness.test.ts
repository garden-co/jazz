import { describe, it, expect } from "vitest";
import { defineSuite, runSuites, slugify, summarize } from "./harness";
import type { TestResult } from "./types";

describe("slugify", () => {
  it("normalizes names to stable testID segments", () => {
    expect(slugify("Writes 100 todos!")).toBe("writes-100-todos");
  });
});

describe("runSuites", () => {
  it("runs sequentially; records pass/fail/timeout; isolates + shuts down a db per test", async () => {
    const appIds: string[] = [];
    let shutdowns = 0;

    const createDb = async (config: { appId: string }) => {
      appIds.push(config.appId);
      return {
        all: async () => [],
        shutdown: async () => {
          shutdowns += 1;
        },
      } as any;
    };

    const updates: TestResult[][] = [];
    const onUpdate = (results: TestResult[]) => updates.push(results.map((r) => ({ ...r })));

    const alpha = defineSuite("alpha", ({ test }) => {
      test("passes", async ({ expect: e }) => {
        e(1).toBe(1);
      });
      test("fails", async ({ expect: e }) => {
        e(1).toBe(2);
      });
    });
    const beta = defineSuite("beta", ({ test }) => {
      test("hangs", async () => {
        await new Promise(() => {});
      });
    });

    const results = await runSuites([alpha, beta], { createDb, onUpdate, perTestTimeoutMs: 100 });

    expect(results.map((r) => r.status)).toEqual(["passed", "failed", "failed"]);
    expect(results.map((r) => r.slug)).toEqual(["alpha-passes", "alpha-fails", "beta-hangs"]);
    expect(results[1]!.error).toBeTruthy();
    expect(results[2]!.error).toMatch(/Timeout/i);

    expect(new Set(appIds).size).toBe(3);
    expect(shutdowns).toBe(3);
    expect(results.every((r) => typeof r.durationMs === "number")).toBe(true);

    expect(summarize(results)).toEqual({
      total: 3,
      passed: 1,
      failed: 2,
      done: true,
      allPassed: false,
    });

    const sawRunningBeforeTerminal = updates.some((snapshot) =>
      snapshot.some((r) => r.status === "running"),
    );
    expect(sawRunningBeforeTerminal).toBe(true);
  });

  it("summarize reports allPassed when everything passes", async () => {
    const createDb = async () => ({ all: async () => [], shutdown: async () => {} }) as any;
    const suite = defineSuite("s", ({ test }) => {
      test("ok", async ({ expect: e }) => {
        e(true).toBeTruthy();
      });
    });
    const results = await runSuites([suite], { createDb, onUpdate: () => {} });
    expect(summarize(results).allPassed).toBe(true);
  });
});
