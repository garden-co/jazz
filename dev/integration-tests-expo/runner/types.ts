export type TestStatus = "pending" | "running" | "passed" | "failed";

export interface TestResult {
  suite: string;
  name: string;
  slug: string;
  status: TestStatus;
  error?: string;
  currentStep?: string;
  durationMs?: number;
}

export interface SuiteSummary {
  total: number;
  passed: number;
  failed: number;
  /** Every test has reached a terminal (passed/failed) status. */
  done: boolean;
  /** done && no failures. This is what Maestro keys off of. */
  allPassed: boolean;
}
