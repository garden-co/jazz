/**
 * Measurements of whole example apps that CodSpeed does not run: how long a
 * user waits for content, and how much data an app holds before its server
 * or client runs out of its memory budget. Each set records the build and
 * method it came from; re-run the named harness and replace the numbers.
 */

export type LoadTime = {
  /** What the user does, and what counts as "loaded". */
  scenario: string;
  /** Median of the local runs, in milliseconds. */
  localMs: number;
  /** One run with 100 ms round trips to the API and the sync server. */
  rttMs: number | null;
};

export type LoadTimeSet = {
  measured: string;
  build: string;
  method: string;
  /** Repository-relative harness directory, once it is on main. */
  harness: string | null;
  byExample: Record<string, LoadTime[]>;
};

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

export const loadTimes: LoadTimeSet = {
  measured: "",
  build: "",
  method: "",
  harness: null,
  byExample: {},
};

export const memoryCeilings: MemoryCeilingSet = {
  measured: "2026-10-13",
  build: "main 799f356b",
  budgets:
    "1 GiB for the server (a hosted app's default memory limit) and 512 MiB of client growth over its idle baseline",
  method:
    "Real app users act through the app's deployed permission policies against that build's server binary, while the data grows by half each step until a side crosses its budget.",
  harness: "dev/benchmarks/memory-ceiling",
  byExample: {
    "big-label": [
      {
        scenario: "Many labels on one server",
        ceiling: 396,
        unit: "labels",
        previous: { label: "alpha.59", ceiling: 218 },
        firstToCross:
          "The server, while a member edits a release under the label's policies.",
      },
      {
        scenario: "One label growing",
        ceiling: 6329,
        unit: "releases",
        previous: { label: "alpha.59", ceiling: 4219 },
        firstToCross:
          "The server, on a restart with nine of the label's members reconnecting.",
      },
    ],
  },
};
