/** Documentation of ONE timed harness iteration, not inferred from table size. */
export type BenchmarkMetadata = {
  name: string;
  title: string;
  description: string;
  fixture: string;
  storage: string;
  includes: string[];
  excludes: string[];
  work: { count: number; unit: string; explanation: string };
  source: string;
};

// This is a reviewed documentation revision, not a claim that every historical
// run used identical timer boundaries. Source links also resolve at each run SHA.
export const metadataRevision = {
  version: 1,
  reviewedCommit: "ed181376b8837833593743f9c0bc5c534c89bc88",
};

export function throughput(seconds: number, work: BenchmarkMetadata["work"]): number | null {
  if (!Number.isFinite(seconds) || seconds <= 0 || !Number.isFinite(work.count) || work.count <= 0)
    return null;
  return work.count / seconds;
}
