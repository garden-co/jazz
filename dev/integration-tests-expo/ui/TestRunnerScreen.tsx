import * as React from "react";
import { ScrollView, StyleSheet, Text, View } from "react-native";
import { createDb } from "jazz-tools/react-native";
import { initialResultsForSuites, runSuites, summarize, type Suite } from "../runner/harness";
import type { TestResult, TestStatus } from "../runner/types";

export function TestRunnerScreen({ suites }: { suites: Suite[] }) {
  const [results, setResults] = React.useState<TestResult[]>(() => initialResultsForSuites(suites));
  const started = React.useRef(false);

  React.useLayoutEffect(() => {
    if (started.current) return;
    started.current = true;
    void runSuites(suites, {
      createDb: (config) => createDb({ appId: config.appId }),
      onUpdate: (next) => setResults([...next]),
    }).catch((error) => {
      setResults([
        {
          suite: "runner",
          name: "startup",
          slug: "runner-startup",
          status: "failed",
          error: error instanceof Error ? error.message : String(error),
          durationMs: 0,
        },
      ]);
    });
  }, [suites]);

  const summary = summarize(results);
  const status = summary.done ? (summary.allPassed ? "PASSED" : "FAILED") : "RUNNING";

  return (
    <View style={styles.container}>
      <Text style={styles.title}>Jazz RN integration tests</Text>
      <Text testID="suite-status" style={[styles.status, statusColor(status)]}>
        {status} · {summary.passed}/{summary.total}
      </Text>

      <ScrollView style={styles.list} contentContainerStyle={styles.listContent}>
        {results.map((r) => (
          <View key={r.slug} testID={`test-row-${r.slug}`} style={styles.row}>
            <Text style={[styles.badge, badgeColor(r.status)]}>{badgeText(r.status)}</Text>
            <View style={styles.rowBody}>
              <Text style={styles.rowName}>
                {r.suite} › {r.name}
              </Text>
              {r.status === "failed" && r.error ? (
                <Text testID={`test-error-${r.slug}`} style={styles.error}>
                  {r.error}
                </Text>
              ) : null}
              {r.status === "running" && r.currentStep ? (
                <Text testID={`test-progress-${r.slug}`} style={styles.progress}>
                  {r.currentStep}
                </Text>
              ) : null}
            </View>
          </View>
        ))}
      </ScrollView>

      {summary.done && summary.allPassed ? (
        <View testID="suite-passed" style={styles.bannerPass}>
          <Text style={styles.bannerText}>ALL {summary.total} PASSED</Text>
        </View>
      ) : null}

      {summary.done && !summary.allPassed ? (
        <View testID="suite-failed" style={styles.bannerFail}>
          <Text style={styles.bannerText}>
            {summary.failed} FAILED: {failedNames(results)}
          </Text>
        </View>
      ) : null}
    </View>
  );
}

function failedNames(results: TestResult[]): string {
  return results
    .filter((r) => r.status === "failed")
    .map((r) => r.name)
    .join(", ");
}

function badgeText(status: TestStatus): string {
  switch (status) {
    case "passed":
      return "PASS";
    case "failed":
      return "FAIL";
    case "running":
      return "RUN";
    default:
      return "…";
  }
}

function statusColor(status: string) {
  if (status === "PASSED") return { color: "#15803d" };
  if (status === "FAILED") return { color: "#b91c1c" };
  return { color: "#1d4ed8" };
}

function badgeColor(status: TestStatus) {
  switch (status) {
    case "passed":
      return { backgroundColor: "#15803d" };
    case "failed":
      return { backgroundColor: "#b91c1c" };
    case "running":
      return { backgroundColor: "#1d4ed8" };
    default:
      return { backgroundColor: "#9ca3af" };
  }
}

const styles = StyleSheet.create({
  container: { flex: 1, paddingHorizontal: 16, paddingTop: 12, gap: 8 },
  title: { fontSize: 20, fontWeight: "700", color: "#111827" },
  status: { fontSize: 18, fontWeight: "700" },
  list: { flex: 1 },
  listContent: { gap: 6, paddingVertical: 8 },
  row: { flexDirection: "row", alignItems: "flex-start", gap: 8 },
  rowBody: { flex: 1 },
  rowName: { fontSize: 14, color: "#111827" },
  badge: {
    color: "#fff",
    fontSize: 11,
    fontWeight: "700",
    paddingHorizontal: 6,
    paddingVertical: 2,
    borderRadius: 4,
    overflow: "hidden",
    minWidth: 44,
    textAlign: "center",
  },
  error: { fontSize: 12, color: "#b91c1c", marginTop: 2 },
  progress: { fontSize: 12, color: "#4b5563", marginTop: 2 },
  bannerPass: { backgroundColor: "#dcfce7", padding: 12, borderRadius: 8 },
  bannerFail: { backgroundColor: "#fee2e2", padding: 12, borderRadius: 8 },
  bannerText: { fontSize: 16, fontWeight: "700", color: "#111827" },
});
