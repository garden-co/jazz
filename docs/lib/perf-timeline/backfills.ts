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
    receipts: [
      {
        runId: "6aa81939e6139cee3e70f236",
        resultId: "6aa81db9ad9a6239bf751fa7",
        benchmarkName: "first_sync_27518_rocksdb",
      },
      {
        runId: "6aa81939e6139cee3e70f236",
        resultId: "6aa81db8ad9a6239bf751fa2",
        benchmarkName: "batch_update_1350_rocksdb",
      },
      {
        runId: "6aa81939e6139cee3e70f236",
        resultId: "6aa81db8ad9a6239bf751fa3",
        benchmarkName: "reopen_1500_rocksdb",
      },
      {
        runId: "6aa81939e6139cee3e70f236",
        resultId: "6aa81db8ad9a6239bf751fa4",
        benchmarkName: "sequential_update_1350_rocksdb",
      },
      {
        runId: "6aa81939e6139cee3e70f236",
        resultId: "6aa81db8ad9a6239bf751fa5",
        benchmarkName: "sequential_insert_1350_rocksdb",
      },
    ],
  },
  {
    releaseTag: "v2.0.0-alpha.55",
    engineSha: "11738be1b2b442ffb0ea37c3c3891d538ec02251",
    harnessSha: "59d2b7ea4902188d0167721256ca0a8e278bd023",
    harnessSourceSha: "1a5add74c7da1509049ffedaf7f663b2cbd36c6e",
    effectiveDate: "2026-09-15T22:33:17.491Z",
    dateSource: "npm jazz-tools time[2.0.0-alpha.55]",
    workflowUrl: "https://github.com/garden-co/jazz/actions/runs/35475965789",
    receipts: [
      {
        runId: "6aaf23470d0b1760bd3d7d04",
        resultId: "6aaf2365ad9a6239bfb21bd5",
        benchmarkName: "owner_or_org_policy_org_page50[10000]",
      },
      {
        runId: "6aaf23470d0b1760bd3d7d04",
        resultId: "6aaf2365ad9a6239bfb21bd6",
        benchmarkName: "owner_or_org_policy_org_page50[100000]",
      },
      {
        runId: "6aaf23470d0b1760bd3d7d04",
        resultId: "6aaf2365ad9a6239bfb21bd7",
        benchmarkName: "owner_or_org_policy_owner_page50[10000]",
      },
      {
        runId: "6aaf23470d0b1760bd3d7d04",
        resultId: "6aaf2365ad9a6239bfb21bd8",
        benchmarkName: "owner_or_org_policy_owner_page50[100000]",
      },
      {
        runId: "6aaf23470d0b1760bd3d7d04",
        resultId: "6aaf2365ad9a6239bfb21bd9",
        benchmarkName: "owner_policy_page50[10000]",
      },
      {
        runId: "6aaf23470d0b1760bd3d7d04",
        resultId: "6aaf2365ad9a6239bfb21bda",
        benchmarkName: "owner_policy_page50[100000]",
      },
      {
        runId: "6aaf23470d0b1760bd3d7d04",
        resultId: "6aaf2365ad9a6239bfb21bdb",
        benchmarkName: "policy_free_org_page50[10000]",
      },
      {
        runId: "6aaf23470d0b1760bd3d7d04",
        resultId: "6aaf2365ad9a6239bfb21bdc",
        benchmarkName: "policy_free_org_page50[100000]",
      },
      {
        runId: "6aaf23470d0b1760bd3d7d04",
        resultId: "6aaf2365ad9a6239bfb21bdd",
        benchmarkName: "policy_free_owner_page50[10000]",
      },
      {
        runId: "6aaf23470d0b1760bd3d7d04",
        resultId: "6aaf2365ad9a6239bfb21bde",
        benchmarkName: "policy_free_owner_page50[100000]",
      },
      {
        runId: "6aaf23470d0b1760bd3d7d04",
        resultId: "6aaf2365ad9a6239bfb21bdf",
        benchmarkName: "subscribe_owner_or_org_policy_org_page50[10000]",
      },
      {
        runId: "6aaf23470d0b1760bd3d7d04",
        resultId: "6aaf2365ad9a6239bfb21be0",
        benchmarkName: "subscribe_owner_or_org_policy_org_page50[100000]",
      },
      {
        runId: "6aaf23470d0b1760bd3d7d04",
        resultId: "6aaf2365ad9a6239bfb21be1",
        benchmarkName: "subscribe_owner_or_org_policy_owner_page50[10000]",
      },
      {
        runId: "6aaf23470d0b1760bd3d7d04",
        resultId: "6aaf2365ad9a6239bfb21be2",
        benchmarkName: "subscribe_owner_or_org_policy_owner_page50[100000]",
      },
      {
        runId: "6aaf23470d0b1760bd3d7d04",
        resultId: "6aaf2365ad9a6239bfb21be3",
        benchmarkName: "subscribe_owner_policy_page50[10000]",
      },
      {
        runId: "6aaf23470d0b1760bd3d7d04",
        resultId: "6aaf2365ad9a6239bfb21be4",
        benchmarkName: "subscribe_owner_policy_page50[100000]",
      },
      {
        runId: "6aaf23470d0b1760bd3d7d04",
        resultId: "6aaf2365ad9a6239bfb21be5",
        benchmarkName: "subscribe_policy_free_org_page50[10000]",
      },
      {
        runId: "6aaf23470d0b1760bd3d7d04",
        resultId: "6aaf2365ad9a6239bfb21be6",
        benchmarkName: "subscribe_policy_free_org_page50[100000]",
      },
      {
        runId: "6aaf23470d0b1760bd3d7d04",
        resultId: "6aaf2365ad9a6239bfb21be7",
        benchmarkName: "subscribe_policy_free_owner_page50[10000]",
      },
      {
        runId: "6aaf23470d0b1760bd3d7d04",
        resultId: "6aaf2365ad9a6239bfb21be8",
        benchmarkName: "subscribe_policy_free_owner_page50[100000]",
      },
    ],
  },
];
