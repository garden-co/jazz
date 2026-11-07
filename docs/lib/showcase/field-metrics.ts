/**
 * Measurements of whole example apps that CodSpeed does not run: how much data
 * an app holds before its server or client runs out of its memory budget.
 * Load times are on CodSpeed (dev/benchmarks/example-load-times) and come from
 * the perf timeline like every other benchmark. Each set records the build and
 * method it came from; re-run the named harness and replace the numbers.
 */

export type MemoryCeiling = {
  /** The data shape that grows. */
  scenario: string;
  /** The most data that stayed under both budgets, in the app's own unit. */
  ceiling: number;
  unit: string;
  /** The same ceiling on the previous release, for comparison. */
  previous: { label: string; ceiling: number } | null;
  /** Which side crossed its budget first, and doing what. */
  firstToCross: string;
};

export type MemoryCeilingSet = {
  measured: string;
  build: string;
  budgets: string;
  method: string;
  /** Repository-relative harness directory, once it is on main. */
  harness: string | null;
  byExample: Record<string, MemoryCeiling[]>;
};

export const memoryCeilings: MemoryCeilingSet = {
  measured: "2026-10-13",
  build: "main 799f356b",
  budgets:
    "1 GiB for the server (a hosted app's default memory limit) and 512 MiB of client growth over its idle baseline",
  method:
    "Real app users act through the app's deployed permission policies against that build's server binary, while the data grows by half each step until a side crosses its budget.",
  // dev/benchmarks/memory-ceiling once #4029 is on main.
  harness: null,
  byExample: {
    "big-label": [
      {
        scenario: "Many labels on one server",
        ceiling: 396,
        unit: "labels",
        previous: { label: "alpha.59", ceiling: 218 },
        firstToCross: "The server, while a member edits a release under the label's policies.",
      },
      {
        scenario: "One label growing",
        ceiling: 6329,
        unit: "releases",
        previous: { label: "alpha.59", ceiling: 4219 },
        firstToCross: "The server, on a restart with nine of the label's members reconnecting.",
      },
    ],
  },
};
