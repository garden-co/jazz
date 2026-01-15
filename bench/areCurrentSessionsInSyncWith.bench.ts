import { readFileSync } from "node:fs";
import { join } from "node:path";
import { areCurrentSessionsInSyncWith } from "cojson/src/knownState.js";

interface SubsetCoValueRow {
  id: string;
  originKnownState: {
    id: string;
    header: boolean;
    sessions: Record<string, number>;
  };
  currentKnownState: {
    id: string;
    header: boolean;
    sessions: Record<string, number>;
  };
}

function loadSubsetCoValuesFiles(): SubsetCoValueRow[] {
  const files = [
    "subset-covalues.ndjson",
    "subset-covalues(1).ndjson",
    "subset-covalues(2).ndjson",
    "subset-covalues(3).ndjson",
  ];

  const allRows: SubsetCoValueRow[] = [];

  for (const file of files) {
    const filePath = join(process.cwd(), "..", file);
    try {
      const content = readFileSync(filePath, "utf-8");
      const lines = content
        .trim()
        .split("\n")
        .filter((line) => line.trim());

      for (const line of lines) {
        try {
          const row = JSON.parse(line) as SubsetCoValueRow;
          allRows.push(row);
        } catch (e) {
          console.warn(`Failed to parse line in ${file}:`, e);
        }
      }
    } catch (e) {
      console.warn(`Failed to read ${file}:`, e);
    }
  }

  return allRows;
}

/**
 * Converts a Record<string, number> to an array format where
 * even positions are keys (strings) and odd positions are values (numbers).
 * Entries are sorted by key to ensure consistent ordering for comparison.
 */
function sessionsToArray(
  sessions: Record<string, number>,
): (string | number)[] {
  const arr: (string | number)[] = [];
  const entries = Object.entries(sessions).sort(([a], [b]) =>
    a.localeCompare(b),
  );
  for (const [key, value] of entries) {
    arr.push(key, value);
  }
  return arr;
}

/**
 * Compares two session arrays to check if all current sessions are in sync.
 * Arrays are in format: [key1, value1, key2, value2, ...]
 */
function areCurrentSessionsInSyncWithArrayed(
  current: (string | number)[],
  target: (string | number)[],
): boolean {
  // Simple array comparison - check if arrays are equal
  if (current.length !== target.length) {
    return false;
  }
  for (let i = 0; i < current.length; i++) {
    if (current[i] !== target[i]) {
      return false;
    }
  }
  return true;
}

function benchmark() {
  console.log("Loading subset-covalues files...");
  const rows = loadSubsetCoValuesFiles();
  console.log(`Loaded ${rows.length} rows\n`);

  if (rows.length === 0) {
    console.error("No rows loaded. Exiting.");
    process.exit(1);
  }

  // Pre-calculate session statistics
  console.log("Pre-calculating session statistics...");
  let totalOriginSessions = 0;
  let totalCurrentSessions = 0;
  for (const row of rows) {
    totalOriginSessions += Object.keys(row.originKnownState.sessions).length;
    totalCurrentSessions += Object.keys(row.currentKnownState.sessions).length;
  }

  // Statistics
  let totalComparisons = 0;
  let totalSyncResults = 0;
  let totalNotSyncResults = 0;

  // Warmup
  console.log("Warming up...");
  for (const row of rows) {
    areCurrentSessionsInSyncWith(
      row.originKnownState.sessions,
      row.currentKnownState.sessions,
    );
  }

  // Actual benchmark
  console.log("Running benchmark...\n");
  const startTime = performance.now();

  for (const row of rows) {
    const originSessions = row.originKnownState.sessions;
    const currentSessions = row.currentKnownState.sessions;

    totalComparisons++;

    const result = areCurrentSessionsInSyncWith(
      originSessions,
      currentSessions,
    );

    if (result) {
      totalSyncResults++;
    } else {
      totalNotSyncResults++;
    }
  }

  const endTime = performance.now();
  const totalTime = endTime - startTime;
  const avgTime = totalTime / totalComparisons;

  // Results
  console.log("=".repeat(60));
  console.log("Benchmark Results");
  console.log("=".repeat(60));
  console.log(
    `Total known state comparisons: ${totalComparisons.toLocaleString()}`,
  );
  console.log(`Total time: ${totalTime.toFixed(2)} ms`);
  console.log(`Average time per comparison: ${avgTime.toFixed(4)} ms`);
  console.log(
    `Comparisons per second: ${((totalComparisons * 1000) / totalTime).toLocaleString(undefined, { maximumFractionDigits: 0 })}`,
  );
  console.log();
  console.log("Session Statistics:");
  console.log(
    `  Total origin sessions: ${totalOriginSessions.toLocaleString()}`,
  );
  console.log(
    `  Total current sessions: ${totalCurrentSessions.toLocaleString()}`,
  );
  console.log(
    `  Average origin sessions per comparison: ${(totalOriginSessions / totalComparisons).toFixed(2)}`,
  );
  console.log(
    `  Average current sessions per comparison: ${(totalCurrentSessions / totalComparisons).toFixed(2)}`,
  );
  console.log();
  console.log("Sync Results:");
  console.log(
    `  In sync: ${totalSyncResults.toLocaleString()} (${((totalSyncResults / totalComparisons) * 100).toFixed(2)}%)`,
  );
  console.log(
    `  Not in sync: ${totalNotSyncResults.toLocaleString()} (${((totalNotSyncResults / totalComparisons) * 100).toFixed(2)}%)`,
  );
  console.log("=".repeat(60));
}

function benchmarkArrayed() {
  console.log("\n");
  console.log("=".repeat(60));
  console.log("Array-based Benchmark");
  console.log("=".repeat(60));
  console.log("Loading subset-covalues files...");
  const rows = loadSubsetCoValuesFiles();
  console.log(`Loaded ${rows.length} rows\n`);

  if (rows.length === 0) {
    console.error("No rows loaded. Exiting.");
    process.exit(1);
  }

  // Pre-calculate session statistics
  console.log("Pre-calculating session statistics...");
  let totalOriginSessions = 0;
  let totalCurrentSessions = 0;
  for (const row of rows) {
    totalOriginSessions += Object.keys(row.originKnownState.sessions).length;
    totalCurrentSessions += Object.keys(row.currentKnownState.sessions).length;
  }

  // Convert sessions to array format
  console.log("Converting sessions to array format...");
  const arrayedRows = rows.map((row) => ({
    originSessionsArray: sessionsToArray(row.originKnownState.sessions),
    currentSessionsArray: sessionsToArray(row.currentKnownState.sessions),
  }));

  // Statistics
  let totalComparisons = 0;
  let totalSyncResults = 0;
  let totalNotSyncResults = 0;

  // Warmup
  console.log("Warming up...");
  for (const { originSessionsArray, currentSessionsArray } of arrayedRows) {
    areCurrentSessionsInSyncWithArrayed(
      originSessionsArray,
      currentSessionsArray,
    );
  }

  // Actual benchmark
  console.log("Running benchmark...\n");
  const startTime = performance.now();

  for (const { originSessionsArray, currentSessionsArray } of arrayedRows) {
    totalComparisons++;

    const result = areCurrentSessionsInSyncWithArrayed(
      originSessionsArray,
      currentSessionsArray,
    );

    if (result) {
      totalSyncResults++;
    } else {
      totalNotSyncResults++;
    }
  }

  const endTime = performance.now();
  const totalTime = endTime - startTime;
  const avgTime = totalTime / totalComparisons;

  // Results
  console.log("=".repeat(60));
  console.log("Array-based Benchmark Results");
  console.log("=".repeat(60));
  console.log(
    `Total known state comparisons: ${totalComparisons.toLocaleString()}`,
  );
  console.log(`Total time: ${totalTime.toFixed(2)} ms`);
  console.log(`Average time per comparison: ${avgTime.toFixed(4)} ms`);
  console.log(
    `Comparisons per second: ${((totalComparisons * 1000) / totalTime).toLocaleString(undefined, { maximumFractionDigits: 0 })}`,
  );
  console.log();
  console.log("Session Statistics:");
  console.log(
    `  Total origin sessions: ${totalOriginSessions.toLocaleString()}`,
  );
  console.log(
    `  Total current sessions: ${totalCurrentSessions.toLocaleString()}`,
  );
  console.log(
    `  Average origin sessions per comparison: ${(totalOriginSessions / totalComparisons).toFixed(2)}`,
  );
  console.log(
    `  Average current sessions per comparison: ${(totalCurrentSessions / totalComparisons).toFixed(2)}`,
  );
  console.log();
  console.log("Sync Results:");
  console.log(
    `  In sync: ${totalSyncResults.toLocaleString()} (${((totalSyncResults / totalComparisons) * 100).toFixed(2)}%)`,
  );
  console.log(
    `  Not in sync: ${totalNotSyncResults.toLocaleString()} (${((totalNotSyncResults / totalComparisons) * 100).toFixed(2)}%)`,
  );
  console.log("=".repeat(60));
}

benchmark();
benchmarkArrayed();
