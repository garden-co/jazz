import type { Db } from "jazz-tools/react-native";
import { expect, type Expect } from "./expect";
import * as support from "./support";
import type { SuiteSummary, TestResult } from "./types";

export interface TestCtx {
  db: Db;
  expect: Expect;
  waitForQuery: typeof support.waitForQuery;
  waitForCondition: typeof support.waitForCondition;
  withTimeout: typeof support.withTimeout;
  sleep: typeof support.sleep;
  uniqueAppId: typeof support.uniqueAppId;
  step: (message: string) => Promise<void>;
}

export type TestBody = (ctx: TestCtx) => Promise<void>;

export interface Suite {
  name: string;
  tests: { name: string; body: TestBody }[];
}

export interface RunnerDeps {
  /** Injected so the runner stays free of RN imports (Node-unit-testable). */
  createDb: (config: { appId: string }) => Promise<Db>;
  onUpdate: (results: TestResult[]) => void;
  perTestTimeoutMs?: number;
}

export function slugify(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

export function defineSuite(
  name: string,
  register: (api: { test: (name: string, body: TestBody) => void }) => void,
): Suite {
  const tests: { name: string; body: TestBody }[] = [];
  register({ test: (testName, body) => tests.push({ name: testName, body }) });
  return { name, tests };
}

export function summarize(results: TestResult[]): SuiteSummary {
  const total = results.length;
  const passed = results.filter((r) => r.status === "passed").length;
  const failed = results.filter((r) => r.status === "failed").length;
  const done =
    results.length > 0 && results.every((r) => r.status === "passed" || r.status === "failed");
  return { total, passed, failed, done, allPassed: done && failed === 0 };
}

function startupFailure(message: string): TestResult {
  return {
    suite: "runner",
    name: "startup",
    slug: "runner-startup",
    status: "failed",
    error: message,
    durationMs: 0,
  };
}

function collectItems(suites: Suite[]) {
  const items: { body: TestBody; result: TestResult }[] = [];
  for (const suite of suites) {
    for (const t of suite.tests) {
      items.push({
        body: t.body,
        result: {
          suite: suite.name,
          name: t.name,
          slug: `${slugify(suite.name)}-${slugify(t.name)}`,
          status: "pending" as const,
        } as TestResult,
      });
    }
  }
  return items;
}

export function initialResultsForSuites(suites: Suite[]): TestResult[] {
  const results = collectItems(suites).map((i) => i.result);
  return results.length > 0 ? results : [startupFailure("No tests registered")];
}

export async function runSuites(suites: Suite[], deps: RunnerDeps): Promise<TestResult[]> {
  const perTestTimeoutMs = deps.perTestTimeoutMs ?? 30_000;

  const items = collectItems(suites);
  if (items.length === 0) {
    const results = [startupFailure("No tests registered")];
    deps.onUpdate([...results]);
    return results;
  }

  const results = items.map((i) => i.result);
  deps.onUpdate([...results]);

  for (const { body, result } of items) {
    result.status = "running";
    result.currentStep = "starting";
    deps.onUpdate([...results]);

    const started = Date.now();
    let db: Db | undefined;
    const step = async (message: string) => {
      result.currentStep = message;
      deps.onUpdate([...results]);
      await support.sleep(0);
    };
    try {
      await support.withTimeout(
        (async () => {
          await step("creating db");
          db = await deps.createDb({ appId: support.uniqueAppId(result.slug) });
          await step("running test body");
          const ctx: TestCtx = {
            db,
            expect,
            waitForQuery: support.waitForQuery,
            waitForCondition: support.waitForCondition,
            withTimeout: support.withTimeout,
            sleep: support.sleep,
            uniqueAppId: support.uniqueAppId,
            step,
          };
          await body(ctx);
        })(),
        perTestTimeoutMs,
        `${result.suite} › ${result.name}`,
      );
      result.status = "passed";
      result.currentStep = undefined;
    } catch (error) {
      result.status = "failed";
      result.error = error instanceof Error ? error.message : String(error);
    } finally {
      if (db) {
        try {
          await db.shutdown();
        } catch {
          // best effort — a failing test should still report its own error
        }
      }
      result.durationMs = Date.now() - started;
      deps.onUpdate([...results]);
    }
  }

  return results;
}
