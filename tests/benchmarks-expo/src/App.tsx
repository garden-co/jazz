import React, { useState } from "react";
import {
  View,
  Text,
  TouchableOpacity,
  ScrollView,
  StyleSheet,
  ActivityIndicator,
  SafeAreaView,
} from "react-native";
import { benchmarks, ComparisonResult, formatNumber } from "./benchmarks";

type BenchmarkState = "idle" | "running" | "complete" | "error";

export default function App() {
  const [selectedIndex, setSelectedIndex] = useState(0);
  const [state, setState] = useState<BenchmarkState>("idle");
  const [progress, setProgress] = useState<string>("");
  const [results, setResults] = useState<ComparisonResult[]>([]);
  const [error, setError] = useState<string | null>(null);

  const selectedBenchmark = benchmarks[selectedIndex];

  const runBenchmark = async () => {
    if (!selectedBenchmark) return;

    setState("running");
    setResults([]);
    setError(null);
    setProgress("Starting...");

    try {
      await selectedBenchmark.run(
        (message) => setProgress(message),
        (result) => setResults((prev) => [...prev, result]),
      );
      setState("complete");
    } catch (e) {
      setState("error");
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const getSpeedupColor = (speedup: number) => {
    if (speedup >= 10) return "#22c55e"; // green
    if (speedup >= 5) return "#84cc16"; // lime
    if (speedup >= 2) return "#eab308"; // yellow
    if (speedup >= 1) return "#f97316"; // orange
    return "#ef4444"; // red
  };

  return (
    <SafeAreaView style={styles.container}>
      <ScrollView style={styles.scroll} contentContainerStyle={styles.content}>
        {/* Header */}
        <Text style={styles.title}>Jazz Benchmarks</Text>
        <Text style={styles.subtitle}>React Native Performance Testing</Text>

        {/* Benchmark Selector */}
        <View style={styles.section}>
          <Text style={styles.sectionTitle}>Select Benchmark</Text>
          <View style={styles.selectorContainer}>
            {benchmarks.map((b, i) => (
              <TouchableOpacity
                key={i}
                style={[
                  styles.selectorButton,
                  selectedIndex === i && styles.selectorButtonActive,
                ]}
                onPress={() => setSelectedIndex(i)}
              >
                <Text
                  style={[
                    styles.selectorText,
                    selectedIndex === i && styles.selectorTextActive,
                  ]}
                >
                  {b.config.name}
                </Text>
              </TouchableOpacity>
            ))}
          </View>
        </View>

        {/* Benchmark Info */}
        {selectedBenchmark && (
          <View style={styles.infoCard}>
            <Text style={styles.infoTitle}>
              {selectedBenchmark.config.name}
            </Text>
            <Text style={styles.infoDescription}>
              {selectedBenchmark.config.description}
            </Text>
            <Text style={styles.infoMeta}>
              {selectedBenchmark.config.iterations} iterations •{" "}
              {selectedBenchmark.config.sizes.length} data sizes
            </Text>
          </View>
        )}

        {/* Run Button */}
        <TouchableOpacity
          style={[
            styles.runButton,
            state === "running" && styles.runButtonDisabled,
          ]}
          onPress={runBenchmark}
          disabled={state === "running"}
        >
          {state === "running" ? (
            <View style={styles.runButtonContent}>
              <ActivityIndicator color="#fff" size="small" />
              <Text style={styles.runButtonText}>Running...</Text>
            </View>
          ) : (
            <Text style={styles.runButtonText}>Run Benchmark</Text>
          )}
        </TouchableOpacity>

        {/* Progress */}
        {state === "running" && (
          <View style={styles.progressCard}>
            <Text style={styles.progressText}>{progress}</Text>
          </View>
        )}

        {/* Error */}
        {error && (
          <View style={styles.errorCard}>
            <Text style={styles.errorText}>Error: {error}</Text>
          </View>
        )}

        {/* Results */}
        {results.length > 0 && (
          <View style={styles.section}>
            <Text style={styles.sectionTitle}>Results</Text>

            {/* Results Table Header */}
            <View style={styles.tableHeader}>
              <Text style={[styles.tableHeaderText, styles.colSize]}>Test</Text>
              <Text style={[styles.tableHeaderText, styles.colTime]}>JS</Text>
              <Text style={[styles.tableHeaderText, styles.colTime]}>Rust</Text>
              <Text style={[styles.tableHeaderText, styles.colSpeedup]}>
                Speedup
              </Text>
            </View>

            {/* Results Rows */}
            {results.map((result, i) => (
              <View key={i} style={styles.tableRow}>
                <Text style={[styles.tableCell, styles.colSize]}>
                  {result.size}
                </Text>
                <Text style={[styles.tableCell, styles.colTime]}>
                  {formatNumber(result.baseline.avgMs)}ms
                </Text>
                <Text style={[styles.tableCell, styles.colTime]}>
                  {formatNumber(result.optimized.avgMs)}ms
                </Text>
                <Text
                  style={[
                    styles.tableCell,
                    styles.colSpeedup,
                    { color: getSpeedupColor(result.speedup) },
                  ]}
                >
                  {result.speedup.toFixed(1)}x
                </Text>
              </View>
            ))}

            {/* Summary */}
            {state === "complete" && results.length > 0 && (
              <View style={styles.summaryCard}>
                <Text style={styles.summaryTitle}>Summary</Text>
                <Text style={styles.summaryText}>
                  Average speedup:{" "}
                  <Text style={{ color: "#22c55e", fontWeight: "bold" }}>
                    {(
                      results.reduce((sum, r) => sum + r.speedup, 0) /
                      results.length
                    ).toFixed(1)}
                    x
                  </Text>
                </Text>
                <Text style={styles.summaryText}>
                  Max speedup:{" "}
                  <Text style={{ color: "#22c55e", fontWeight: "bold" }}>
                    {Math.max(...results.map((r) => r.speedup)).toFixed(1)}x
                  </Text>
                </Text>
              </View>
            )}
          </View>
        )}

        {/* Instructions */}
        <View style={styles.instructionsCard}>
          <Text style={styles.instructionsTitle}>Adding New Benchmarks</Text>
          <Text style={styles.instructionsText}>
            1. Create a new file in src/benchmarks/{"\n"}
            2. Implement the BenchmarkSuite interface{"\n"}
            3. Add it to the benchmarks array in index.ts
          </Text>
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: "#0a0a0a",
  },
  scroll: {
    flex: 1,
  },
  content: {
    padding: 20,
    paddingBottom: 40,
  },
  title: {
    fontSize: 28,
    fontWeight: "bold",
    color: "#fff",
    textAlign: "center",
    marginTop: 20,
  },
  subtitle: {
    fontSize: 14,
    color: "#666",
    textAlign: "center",
    marginBottom: 24,
  },
  section: {
    marginBottom: 24,
  },
  sectionTitle: {
    fontSize: 16,
    fontWeight: "600",
    color: "#888",
    marginBottom: 12,
    textTransform: "uppercase",
    letterSpacing: 1,
  },
  selectorContainer: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 8,
  },
  selectorButton: {
    paddingHorizontal: 16,
    paddingVertical: 10,
    borderRadius: 8,
    backgroundColor: "#1a1a1a",
    borderWidth: 1,
    borderColor: "#333",
  },
  selectorButtonActive: {
    backgroundColor: "#3b82f6",
    borderColor: "#3b82f6",
  },
  selectorText: {
    color: "#888",
    fontSize: 14,
  },
  selectorTextActive: {
    color: "#fff",
    fontWeight: "600",
  },
  infoCard: {
    backgroundColor: "#1a1a1a",
    borderRadius: 12,
    padding: 16,
    marginBottom: 20,
  },
  infoTitle: {
    fontSize: 18,
    fontWeight: "bold",
    color: "#fff",
    marginBottom: 8,
  },
  infoDescription: {
    fontSize: 14,
    color: "#aaa",
    marginBottom: 8,
  },
  infoMeta: {
    fontSize: 12,
    color: "#666",
  },
  runButton: {
    backgroundColor: "#3b82f6",
    paddingVertical: 16,
    borderRadius: 12,
    alignItems: "center",
    marginBottom: 20,
  },
  runButtonDisabled: {
    backgroundColor: "#1e40af",
  },
  runButtonContent: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },
  runButtonText: {
    color: "#fff",
    fontSize: 16,
    fontWeight: "600",
  },
  progressCard: {
    backgroundColor: "#1e3a5f",
    borderRadius: 8,
    padding: 12,
    marginBottom: 20,
  },
  progressText: {
    color: "#93c5fd",
    fontSize: 14,
    fontFamily: "monospace",
  },
  errorCard: {
    backgroundColor: "#450a0a",
    borderRadius: 8,
    padding: 12,
    marginBottom: 20,
    borderWidth: 1,
    borderColor: "#ef4444",
  },
  errorText: {
    color: "#fca5a5",
    fontSize: 14,
  },
  tableHeader: {
    flexDirection: "row",
    backgroundColor: "#1a1a1a",
    paddingVertical: 10,
    paddingHorizontal: 12,
    borderRadius: 8,
    marginBottom: 4,
  },
  tableHeaderText: {
    color: "#888",
    fontSize: 12,
    fontWeight: "600",
    textTransform: "uppercase",
  },
  tableRow: {
    flexDirection: "row",
    backgroundColor: "#111",
    paddingVertical: 12,
    paddingHorizontal: 12,
    borderRadius: 8,
    marginBottom: 4,
  },
  tableCell: {
    color: "#fff",
    fontSize: 13,
    fontFamily: "monospace",
  },
  colSize: {
    flex: 2,
  },
  colTime: {
    flex: 1,
    textAlign: "right",
  },
  colSpeedup: {
    flex: 1,
    textAlign: "right",
    fontWeight: "bold",
  },
  summaryCard: {
    backgroundColor: "#14532d",
    borderRadius: 12,
    padding: 16,
    marginTop: 16,
    borderWidth: 1,
    borderColor: "#22c55e",
  },
  summaryTitle: {
    fontSize: 16,
    fontWeight: "bold",
    color: "#fff",
    marginBottom: 8,
  },
  summaryText: {
    fontSize: 14,
    color: "#d1d5db",
    marginBottom: 4,
  },
  instructionsCard: {
    backgroundColor: "#1a1a1a",
    borderRadius: 12,
    padding: 16,
    marginTop: 20,
    borderWidth: 1,
    borderColor: "#333",
    borderStyle: "dashed",
  },
  instructionsTitle: {
    fontSize: 14,
    fontWeight: "600",
    color: "#888",
    marginBottom: 8,
  },
  instructionsText: {
    fontSize: 12,
    color: "#666",
    fontFamily: "monospace",
    lineHeight: 20,
  },
});
