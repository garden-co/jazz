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
  measured: "2026-10-13",
  build: "main 75c07b45",
  method:
    "Production builds of each app with their own sync server, assets served compressed as from a CDN, in headless Chromium on a 4-core machine. Local is the median of three runs; the latency column is one run with 100 ms round trips to the app's API and sync server.",
  harness: null,
  byExample: {
    "band-book": [
      {
        scenario: "Sign up and see the starter band",
        localMs: 3963,
        rttMs: 4688,
      },
      { scenario: "Reload and see the page", localMs: 1769, rttMs: 1982 },
      {
        scenario: "Open an invite and see the shared content",
        localMs: 3093,
        rttMs: 3837,
      },
      {
        scenario: "Invited member reloads and sees the page",
        localMs: 1668,
        rttMs: 1558,
      },
      {
        scenario: "An edit appears on another device",
        localMs: 284,
        rttMs: 438,
      },
    ],
    "band-chat": [
      {
        scenario: "Sign up until the app is ready",
        localMs: 3966,
        rttMs: 4394,
      },
      {
        scenario: "Reload and see the latest message",
        localMs: 3218,
        rttMs: 3179,
      },
      {
        scenario: "Open an invite and see the join prompt",
        localMs: 2702,
        rttMs: 2827,
      },
      { scenario: "Get admitted and see the room", localMs: 1978, rttMs: 1950 },
      {
        scenario: "Invited member reloads and sees the latest message",
        localMs: 1962,
        rttMs: 2348,
      },
      {
        scenario: "A message appears on another device",
        localMs: 559,
        rttMs: 461,
      },
    ],
    "big-label": [
      { scenario: "Sign up and see the overview", localMs: 3980, rttMs: 4324 },
      { scenario: "Reload and see the releases", localMs: 1617, rttMs: 1714 },
      {
        scenario: "Invited member picks the label and sees its releases",
        localMs: 1367,
        rttMs: 1392,
      },
      {
        scenario: "Invited member reloads and sees the releases",
        localMs: 1832,
        rttMs: 2060,
      },
    ],
    "epic-drop": [
      {
        scenario: "First visit until the folders show",
        localMs: 1716,
        rttMs: 2092,
      },
      { scenario: "Reload and see the files", localMs: 1466, rttMs: 1445 },
      {
        scenario: "Join through an invite and see the shared content",
        localMs: 1480,
        rttMs: 1621,
      },
      {
        scenario: "Invited member reloads and sees the files",
        localMs: 1810,
        rttMs: 1425,
      },
      {
        scenario: "Upload a 3 MB file until it is stored",
        localMs: 3266,
        rttMs: 2913,
      },
      {
        scenario: "A change appears on another device",
        localMs: 548,
        rttMs: 986,
      },
    ],
    jamazon: [
      {
        scenario: "First anonymous visit until search is usable",
        localMs: 1708,
        rttMs: 1797,
      },
      {
        scenario: "First anonymous visit until a product shows",
        localMs: 2759,
        rttMs: 3346,
      },
      { scenario: "Sign up and see the store", localMs: 1252, rttMs: 1852 },
      {
        scenario: "Open a second device and see the cart",
        localMs: 1389,
        rttMs: 1452,
      },
      {
        scenario: "Return visit until a product shows",
        localMs: 1277,
        rttMs: 1376,
      },
      { scenario: "Reload and see the cart", localMs: 1203, rttMs: 1188 },
    ],
    "poster-shop": [
      { scenario: "Sign up and see the poster", localMs: 4366, rttMs: 5061 },
      { scenario: "Reload and see the poster", localMs: 2818, rttMs: 2304 },
      {
        scenario: "Open an invite and see the shared poster",
        localMs: 2753,
        rttMs: 3125,
      },
      {
        scenario: "Invited member reloads and sees the poster",
        localMs: 1727,
        rttMs: 1690,
      },
      {
        scenario: "An edit appears on another device",
        localMs: 577,
        rttMs: 538,
      },
    ],
    "record-player": [
      { scenario: "Sign up and see the library", localMs: 2597, rttMs: 2845 },
      { scenario: "Reload and see the albums", localMs: 2055, rttMs: 2781 },
      {
        scenario: "Second user signs up and sees the shared library",
        localMs: 2591,
        rttMs: 2934,
      },
    ],
    "stage-plan": [
      {
        scenario: "First visit until the demo show opens",
        localMs: 3275,
        rttMs: 3378,
      },
      { scenario: "Reload and see the board", localMs: 2619, rttMs: 2639 },
      {
        scenario: "Open an invite and see the show",
        localMs: 3538,
        rttMs: 4375,
      },
      {
        scenario: "Invited member reloads and sees the board",
        localMs: 2574,
        rttMs: 2028,
      },
      {
        scenario: "An edit appears on another device",
        localMs: 320,
        rttMs: 505,
      },
    ],
    wequencer: [
      {
        scenario: "Sign up until a session can be started",
        localMs: 2290,
        rttMs: 2742,
      },
      { scenario: "Reload and see the pads", localMs: 2898, rttMs: 3104 },
      {
        scenario: "Open an invite and see the shared pads",
        localMs: 1353,
        rttMs: 1589,
      },
      {
        scenario: "Invited member reloads and sees the pads",
        localMs: 1757,
        rttMs: 1707,
      },
    ],
    "world-tour": [
      {
        scenario: "First visit until the whole tour shows",
        localMs: 3166,
        rttMs: 3054,
      },
      { scenario: "Reload and see the whole tour", localMs: 2254, rttMs: 2266 },
      {
        scenario: "Open a public link and see the poster",
        localMs: 4181,
        rttMs: 3523,
      },
      {
        scenario: "Open an invite and see the join dialog",
        localMs: 5330,
        rttMs: 3930,
      },
      {
        scenario: "Join the tour and see its stops",
        localMs: 4015,
        rttMs: 2081,
      },
      {
        scenario: "Invited member reloads and sees the tour",
        localMs: 1611,
        rttMs: 1866,
      },
      {
        scenario: "An edit appears on another device",
        localMs: 383,
        rttMs: 351,
      },
    ],
  },
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
