/** Reviewed measurement receipts only; GitHub workflow IDs are not CodSpeed IDs. */
export type HistoricalBackfill = {
  releaseTag: string;
  engineSha: string;
  harnessSha: string;
  harnessSourceSha: string;
  effectiveDate: string;
  dateSource: string;
  workflowUrl: string;
  // Explicit result IDs exclude CodSpeed's carried-forward partial-run values.
  receipts: { runId: string; resultId: string; benchmarkName: string }[];
};

export const historicalBackfills: HistoricalBackfill[] = [
  {
    releaseTag: "v2.0.0-alpha.54",
    engineSha: "7cb8a9b4e088c5810540f221c0d3434069566e6b",
    harnessSha: "7b61d1d9f1eba2bbf37e27b1039d517c92d3672c",
    harnessSourceSha: "a2cd41ba1453a575f4e72a3ed17a1e5276265543",
    effectiveDate: "2026-09-10T04:26:57.178Z",
    dateSource: "npm jazz-tools time[2.0.0-alpha.54]",
    workflowUrl: "https://github.com/garden-co/jazz/actions/runs/34861926044",
    receipts: [],
  },
];
