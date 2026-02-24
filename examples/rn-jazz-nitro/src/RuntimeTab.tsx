import React, { useState, useCallback, useRef } from "react";
import { Platform, ScrollView, StyleSheet, Text, TouchableOpacity, View } from "react-native";
import type { JazzRuntime } from "./jazz-runtime";
import { todoValues, todoUpdate } from "./schema";

interface Props {
  runtime: JazzRuntime;
}

function formatMs(ms: number): string {
  if (ms < 1) return `${(ms * 1000).toFixed(0)}us`;
  return `${ms.toFixed(2)}ms`;
}

function percentile(sortedMs: number[], p: number): number {
  if (sortedMs.length === 0) return 0;
  const idx = Math.ceil((p / 100) * sortedMs.length) - 1;
  return sortedMs[Math.max(0, Math.min(idx, sortedMs.length - 1))];
}

export function RuntimeTab({ runtime }: Props) {
  const [log, setLog] = useState<string[]>(["Runtime explorer ready."]);
  const insertedIds = useRef<string[]>([]);

  const append = useCallback((line: string) => {
    console.log("[runtime]", line);
    setLog((prev) => [...prev, line]);
  }, []);

  const clearLog = useCallback(() => setLog([]), []);

  // --- Schema ---

  const handleGetSchema = useCallback(() => {
    try {
      const t0 = performance.now();
      const json = runtime.getSchemaJson();
      const elapsed = performance.now() - t0;
      const parsed = JSON.parse(json);
      const tables = Object.keys(parsed.tables || {}).join(", ");
      append(`schema tables: [${tables}] (${formatMs(elapsed)})`);
    } catch (e) {
      append(`schema failed: ${e}`);
    }
  }, [runtime, append]);

  const handleGetSchemaHash = useCallback(() => {
    try {
      const t0 = performance.now();
      const hash = runtime.getSchemaHash();
      const elapsed = performance.now() - t0;
      append(`schema hash: ${hash.slice(0, 16)}... (${formatMs(elapsed)})`);
    } catch (e) {
      append(`schema hash failed: ${e}`);
    }
  }, [runtime, append]);

  // --- Utilities ---

  const handleGenerateId = useCallback(() => {
    try {
      const t0 = performance.now();
      const id = runtime.generateId();
      const elapsed = performance.now() - t0;
      append(`id: ${id} (${formatMs(elapsed)})`);
    } catch (e) {
      append(`generateId failed: ${e}`);
    }
  }, [runtime, append]);

  const handleTimestamp = useCallback(() => {
    try {
      const t0 = performance.now();
      const ts = runtime.currentTimestampMs();
      const elapsed = performance.now() - t0;
      append(`timestamp: ${ts} (${formatMs(elapsed)})`);
    } catch (e) {
      append(`timestamp failed: ${e}`);
    }
  }, [runtime, append]);

  // --- CRUD ---

  const handleInsertOne = useCallback(() => {
    try {
      const t0 = performance.now();
      const id = runtime.insert("todos", todoValues(`Item ${Date.now()}`, false));
      runtime.batchedTick();
      const elapsed = performance.now() - t0;
      insertedIds.current.push(id);
      append(`insert: ${id.slice(0, 8)}... (${formatMs(elapsed)})`);
    } catch (e) {
      append(`insert failed: ${e}`);
    }
  }, [runtime, append]);

  const handleQuery = useCallback(async () => {
    try {
      const t0 = performance.now();
      const result = await runtime.query(JSON.stringify({ table: "todos" }), undefined, undefined);
      const elapsed = performance.now() - t0;
      const rows = JSON.parse(result);
      append(`query: ${rows.length} rows (${formatMs(elapsed)})`);
    } catch (e) {
      append(`query failed: ${e}`);
    }
  }, [runtime, append]);

  const handleUpdateLast = useCallback(() => {
    const lastId = insertedIds.current[insertedIds.current.length - 1];
    if (!lastId) {
      append("update: no inserted ids to update");
      return;
    }
    try {
      const t0 = performance.now();
      runtime.update(lastId, todoUpdate({ done: true }));
      runtime.batchedTick();
      const elapsed = performance.now() - t0;
      append(`update: ${lastId.slice(0, 8)}... done=true (${formatMs(elapsed)})`);
    } catch (e) {
      append(`update failed: ${e}`);
    }
  }, [runtime, append]);

  const handleDeleteLast = useCallback(() => {
    const lastId = insertedIds.current.pop();
    if (!lastId) {
      append("delete: no inserted ids to delete");
      return;
    }
    try {
      const t0 = performance.now();
      runtime.deleteRow(lastId);
      runtime.batchedTick();
      const elapsed = performance.now() - t0;
      append(`delete: ${lastId.slice(0, 8)}... (${formatMs(elapsed)})`);
    } catch (e) {
      append(`delete failed: ${e}`);
    }
  }, [runtime, append]);

  // --- Subscribe ---

  const subHandle = useRef<number | null>(null);
  const deltaCount = useRef(0);

  const handleSubscribe = useCallback(() => {
    if (subHandle.current !== null) {
      append("subscribe: already subscribed");
      return;
    }
    try {
      deltaCount.current = 0;
      const h = runtime.subscribe(
        JSON.stringify({ table: "todos" }),
        (_deltaJson: string) => {
          deltaCount.current++;
        },
        undefined,
        undefined,
      );
      subHandle.current = h;
      runtime.batchedTick();
      append(`subscribe: handle=${h}`);
    } catch (e) {
      append(`subscribe failed: ${e}`);
    }
  }, [runtime, append]);

  const handleUnsubscribe = useCallback(() => {
    if (subHandle.current === null) {
      append("unsubscribe: not subscribed");
      return;
    }
    try {
      runtime.unsubscribe(subHandle.current);
      append(`unsubscribe: handle=${subHandle.current}, deltas=${deltaCount.current}`);
      subHandle.current = null;
      deltaCount.current = 0;
    } catch (e) {
      append(`unsubscribe failed: ${e}`);
    }
  }, [runtime, append]);

  // --- Flush ---

  const handleFlush = useCallback(() => {
    try {
      const t0 = performance.now();
      runtime.flush();
      const elapsed = performance.now() - t0;
      append(`flush (${formatMs(elapsed)})`);
    } catch (e) {
      append(`flush failed: ${e}`);
    }
  }, [runtime, append]);

  // --- Stress ---

  const handleStressInsert = useCallback(() => {
    const N = 1000;
    append(`stress insert: ${N} rows...`);
    setTimeout(() => {
      try {
        const latencies: number[] = [];
        const totalT0 = performance.now();
        for (let i = 0; i < N; i++) {
          const t0 = performance.now();
          const id = runtime.insert("todos", todoValues(`Stress ${i}`, false));
          latencies.push(performance.now() - t0);
          insertedIds.current.push(id);
        }
        runtime.batchedTick();
        const totalMs = performance.now() - totalT0;
        latencies.sort((a, b) => a - b);
        append(
          [
            `stress insert ${N} rows`,
            `  total: ${formatMs(totalMs)}  ops/s: ${Math.round((N / totalMs) * 1000)}`,
            `  p50: ${formatMs(percentile(latencies, 50))}  p95: ${formatMs(percentile(latencies, 95))}  p99: ${formatMs(percentile(latencies, 99))}`,
          ].join("\n"),
        );
      } catch (e) {
        append(`stress insert failed: ${e}`);
      }
    }, 0);
  }, [runtime, append]);

  const handleStressQuery = useCallback(async () => {
    append("stress query...");
    try {
      const t0 = performance.now();
      const result = await runtime.query(JSON.stringify({ table: "todos" }), undefined, undefined);
      const elapsed = performance.now() - t0;
      const rows = JSON.parse(result);
      append(`stress query: ${rows.length} rows in ${formatMs(elapsed)}`);
    } catch (e) {
      append(`stress query failed: ${e}`);
    }
  }, [runtime, append]);

  // --- Render ---

  const sections: {
    label: string;
    buttons: { label: string; onPress: () => void; color?: string }[];
  }[] = [
    {
      label: "Schema & Utilities",
      buttons: [
        { label: "Schema", onPress: handleGetSchema },
        { label: "Hash", onPress: handleGetSchemaHash },
        { label: "Gen ID", onPress: handleGenerateId },
        { label: "Timestamp", onPress: handleTimestamp },
      ],
    },
    {
      label: "CRUD",
      buttons: [
        { label: "Insert", onPress: handleInsertOne },
        { label: "Query", onPress: handleQuery },
        { label: "Update Last", onPress: handleUpdateLast },
        { label: "Delete Last", onPress: handleDeleteLast },
      ],
    },
    {
      label: "Subscriptions",
      buttons: [
        { label: "Subscribe", onPress: handleSubscribe },
        { label: "Unsubscribe", onPress: handleUnsubscribe },
      ],
    },
    {
      label: "Persistence",
      buttons: [{ label: "Flush", onPress: handleFlush }],
    },
    {
      label: "Stress",
      buttons: [
        { label: "Insert 1K", onPress: handleStressInsert, color: "#3a1e5f" },
        { label: "Query All", onPress: handleStressQuery, color: "#3a1e5f" },
      ],
    },
  ];

  return (
    <View style={styles.container}>
      {sections.map((section) => (
        <View key={section.label} style={styles.section}>
          <Text style={styles.sectionLabel}>{section.label}</Text>
          <View style={styles.row}>
            {section.buttons.map((btn) => (
              <TouchableOpacity
                key={btn.label}
                style={[styles.btn, btn.color ? { backgroundColor: btn.color } : null]}
                onPress={btn.onPress}
              >
                <Text style={styles.btnText}>{btn.label}</Text>
              </TouchableOpacity>
            ))}
          </View>
        </View>
      ))}

      <View style={styles.logContainer}>
        <View style={styles.logHeader}>
          <Text style={styles.sectionLabel}>Log</Text>
          <TouchableOpacity onPress={clearLog}>
            <Text style={styles.clearText}>Clear</Text>
          </TouchableOpacity>
        </View>
        <ScrollView style={styles.log} contentContainerStyle={styles.logContent}>
          {log.map((line, i) => (
            <Text key={i} style={styles.logLine}>
              {line}
            </Text>
          ))}
        </ScrollView>
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    gap: 8,
  },
  section: {
    gap: 4,
  },
  sectionLabel: {
    color: "#888",
    fontSize: 10,
    fontWeight: "600",
    textTransform: "uppercase",
    letterSpacing: 0.8,
  },
  row: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 6,
  },
  btn: {
    backgroundColor: "#1e3a5f",
    borderRadius: 6,
    paddingVertical: 7,
    paddingHorizontal: 10,
  },
  btnText: {
    color: "#fff",
    fontSize: 12,
    fontWeight: "600",
  },
  logContainer: {
    flex: 1,
    gap: 4,
  },
  logHeader: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
  },
  clearText: {
    color: "#555",
    fontSize: 11,
  },
  log: {
    flex: 1,
    backgroundColor: "#111",
    borderRadius: 6,
  },
  logContent: {
    padding: 8,
    gap: 2,
  },
  logLine: {
    color: "#c8e6c9",
    fontFamily: Platform.OS === "ios" ? "Menlo" : "monospace",
    fontSize: 11,
    lineHeight: 16,
  },
});
