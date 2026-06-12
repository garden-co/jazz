import { describe, it, expect } from "vitest";
import { initialResultsForSuites, defineSuite, runSuites, slugify, summarize } from "./harness";
import type { TestResult } from "./types";

describe("slugify", () => {
  it("normalizes names to stable testID segments", () => {
    expect(slugify("Writes 100 todos!")).toBe("writes-100-todos");
  });
});

describe("runSuites", () => {
  it("initialResultsForSuites exposes pending rows before the runner effect starts", () => {
    const suite = defineSuite("alpha", ({ test }) => {
      test("first", async () => {});
      test("second", async () => {});
    });

    expect(initialResultsForSuites([suite])).toEqual([
      {
        suite: "alpha",
        name: "first",
        slug: "alpha-first",
        status: "pending",
      },
      {
        suite: "alpha",
        name: "second",
        slug: "alpha-second",
        status: "pending",
      },
    ]);
  });

  it("reports a terminal failure when no tests are registered", async () => {
    let createDbCalls = 0;
    const updates: TestResult[][] = [];

    const results = await runSuites([], {
      createDb: async () => {
        createDbCalls += 1;
        return { all: async () => [], shutdown: async () => {} } as any;
      },
      onUpdate: (next) => updates.push(next.map((r) => ({ ...r }))),
    });

    expect(createDbCalls).toBe(0);
    expect(results).toHaveLength(1);
    expect(results[0]!.status).toBe("failed");
    expect(results[0]!.error).toMatch(/No tests registered/);
    expect(summarize(results)).toEqual({
      total: 1,
      passed: 0,
      failed: 1,
      done: true,
      allPassed: false,
    });
    expect(updates.at(-1)).toEqual(results);
  });

  it("times out a hung db creation and continues to later tests", async () => {
    let createDbCalls = 0;
    const suite = defineSuite("alpha", ({ test }) => {
      test("db hangs", async ({ expect: e }) => {
        e(true).toBe(true);
      });
      test("runs after timeout", async ({ expect: e }) => {
        e(true).toBe(true);
      });
    });

    const results = await runSuites([suite], {
      createDb: async () => {
        createDbCalls += 1;
        if (createDbCalls === 1) {
          await new Promise(() => {});
        }
        return { all: async () => [], shutdown: async () => {} } as any;
      },
      onUpdate: () => {},
      perTestTimeoutMs: 100,
    });

    expect(results.map((r) => r.status)).toEqual(["failed", "passed"]);
    expect(results[0]!.error).toMatch(/Timeout/i);
    expect(createDbCalls).toBe(2);
  });

  it("publishes progress steps around db creation and test body execution", async () => {
    const updates: TestResult[][] = [];
    const suite = defineSuite("alpha", ({ test }) => {
      test("reports progress", async ({ expect: e }) => {
        e(true).toBe(true);
      });
    });

    await runSuites([suite], {
      createDb: async () => ({ all: async () => [], shutdown: async () => {} }) as any,
      onUpdate: (next) => updates.push(next.map((r) => ({ ...r }))),
    });

    const progressSteps = updates
      .flatMap((snapshot) => snapshot.map((result) => result.currentStep))
      .filter((step): step is string => !!step);

    expect(progressSteps).toContain("creating db");
    expect(progressSteps).toContain("running test body");
  });

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
