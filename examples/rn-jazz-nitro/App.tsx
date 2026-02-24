/**
 * Jazz2 React Native — Nitro Runtime Example
 *
 * Two-tab app:
 *   1. Todo — a working todo list backed by the full Jazz runtime
 *   2. Runtime — exercises individual runtime methods with timing
 */

import React, { useState, useEffect, useRef, useCallback } from "react";
import { Platform, StyleSheet, Text, TouchableOpacity, View } from "react-native";
import { SafeAreaProvider, useSafeAreaInsets } from "react-native-safe-area-context";
import RNFS from "react-native-fs";

import { getJazzRuntime, type JazzRuntime } from "./src/jazz-runtime";
import { TODO_SCHEMA_JSON } from "./src/schema";
import { TodoTab } from "./src/TodoTab";
import { RuntimeTab } from "./src/RuntimeTab";

type Tab = "todo" | "runtime";

function storagePath(): string {
  return Platform.OS === "ios"
    ? `${RNFS.DocumentDirectoryPath}/jazz-nitro-kv`
    : `${RNFS.ExternalDirectoryPath ?? RNFS.DocumentDirectoryPath}/jazz-nitro-kv`;
}

export default function App() {
  return (
    <SafeAreaProvider>
      <AppInner />
    </SafeAreaProvider>
  );
}

function AppInner() {
  const insets = useSafeAreaInsets();
  const [tab, setTab] = useState<Tab>("todo");
  const [runtime, setRuntime] = useState<JazzRuntime | null>(null);
  const [error, setError] = useState<string | null>(null);
  const tickTimeout = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Initialize runtime on mount
  useEffect(() => {
    try {
      const rt = getJazzRuntime();

      const path = storagePath();
      console.log("[jazz] Opening runtime at:", path);

      rt.open(TODO_SCHEMA_JSON, "jazz-nitro-example", "dev", "main", path, undefined);

      // Register tick callback — Rust calls this when it needs a batched tick.
      // We schedule it via setTimeout so it runs on the next JS event loop turn.
      rt.onBatchedTickNeeded(() => {
        if (tickTimeout.current !== null) return;
        tickTimeout.current = setTimeout(() => {
          tickTimeout.current = null;
          try {
            rt.batchedTick();
          } catch (e) {
            console.warn("[jazz] batchedTick error:", e);
          }
        }, 0);
      });

      setRuntime(rt);
    } catch (e) {
      console.error("[jazz] Failed to initialize runtime:", e);
      setError(String(e));
    }

    return () => {
      if (tickTimeout.current !== null) {
        clearTimeout(tickTimeout.current);
      }
    };
  }, []);

  const handleFlushAndClose = useCallback(() => {
    if (runtime) {
      try {
        runtime.flush();
        runtime.close();
        console.log("[jazz] Runtime closed");
      } catch (e) {
        console.warn("[jazz] Close error:", e);
      }
    }
  }, [runtime]);

  if (error) {
    return (
      <View style={[styles.root, { paddingTop: insets.top, paddingBottom: insets.bottom }]}>
        <Text style={styles.title}>Jazz Nitro Runtime</Text>
        <Text style={styles.errorText}>Failed to initialize: {error}</Text>
      </View>
    );
  }

  if (!runtime) {
    return (
      <View style={[styles.root, { paddingTop: insets.top, paddingBottom: insets.bottom }]}>
        <Text style={styles.title}>Jazz Nitro Runtime</Text>
        <Text style={styles.loadingText}>Initializing...</Text>
      </View>
    );
  }

  return (
    <View style={[styles.root, { paddingTop: insets.top, paddingBottom: insets.bottom }]}>
      <View style={styles.header}>
        <Text style={styles.title}>Jazz Nitro Runtime</Text>
        <TouchableOpacity onPress={handleFlushAndClose}>
          <Text style={styles.closeBtn}>Flush & Close</Text>
        </TouchableOpacity>
      </View>

      <View style={styles.tabs}>
        <TouchableOpacity
          style={[styles.tab, tab === "todo" && styles.tabActive]}
          onPress={() => setTab("todo")}
        >
          <Text style={[styles.tabText, tab === "todo" && styles.tabTextActive]}>Todos</Text>
        </TouchableOpacity>
        <TouchableOpacity
          style={[styles.tab, tab === "runtime" && styles.tabActive]}
          onPress={() => setTab("runtime")}
        >
          <Text style={[styles.tabText, tab === "runtime" && styles.tabTextActive]}>Runtime</Text>
        </TouchableOpacity>
      </View>

      <View style={styles.content}>
        {tab === "todo" ? <TodoTab runtime={runtime} /> : <RuntimeTab runtime={runtime} />}
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  root: {
    flex: 1,
    backgroundColor: "#0d0d0d",
    padding: 12,
    gap: 8,
  },
  header: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
  },
  title: {
    color: "#fff",
    fontSize: 18,
    fontWeight: "700",
  },
  closeBtn: {
    color: "#e55",
    fontSize: 12,
    fontWeight: "600",
  },
  tabs: {
    flexDirection: "row",
    gap: 0,
    borderRadius: 6,
    overflow: "hidden",
    borderWidth: 1,
    borderColor: "#333",
  },
  tab: {
    flex: 1,
    paddingVertical: 8,
    alignItems: "center",
    backgroundColor: "#1a1a1a",
  },
  tabActive: {
    backgroundColor: "#2563eb",
  },
  tabText: {
    color: "#888",
    fontSize: 13,
    fontWeight: "600",
  },
  tabTextActive: {
    color: "#fff",
  },
  content: {
    flex: 1,
  },
  errorText: {
    color: "#e55",
    fontSize: 13,
    textAlign: "center",
    marginTop: 24,
  },
  loadingText: {
    color: "#888",
    fontSize: 13,
    textAlign: "center",
    marginTop: 24,
  },
});
