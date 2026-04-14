import { useCallback, useRef, useState } from "react";
import { Pressable, ScrollView, StyleSheet, Text, TextInput, View } from "react-native";
import { useAll, useDb, useSession } from "jazz-tools/react-native";
import { app } from "../schema";

const BATCH_SIZE = 500;
const PROJECT_COUNT = 20;

const PROJECT_NAMES = [
  "Backend",
  "Frontend",
  "Mobile",
  "Infra",
  "Design",
  "QA",
  "DevOps",
  "Security",
  "Analytics",
  "Platform",
  "Core",
  "SDK",
  "API",
  "Dashboard",
  "Onboarding",
  "Billing",
  "Search",
  "Notifications",
  "Integrations",
  "Docs",
];

const ADJECTIVES = [
  "Quick",
  "Lazy",
  "Happy",
  "Sad",
  "Bright",
  "Dark",
  "Warm",
  "Cool",
  "Bold",
  "Calm",
  "Fierce",
  "Gentle",
  "Loud",
  "Quiet",
  "Sharp",
  "Smooth",
  "Swift",
  "Slow",
  "Tall",
  "Tiny",
];

const NOUNS = [
  "Report",
  "Email",
  "Meeting",
  "Review",
  "Update",
  "Fix",
  "Deploy",
  "Test",
  "Design",
  "Plan",
  "Sprint",
  "Ticket",
  "Feature",
  "Bug",
  "Refactor",
  "Docs",
  "Release",
  "Backup",
  "Audit",
  "Demo",
];

const VERBS = [
  "Write",
  "Send",
  "Schedule",
  "Finish",
  "Prepare",
  "Submit",
  "Review",
  "Approve",
  "Cancel",
  "Rewrite",
  "Debug",
  "Optimize",
  "Migrate",
  "Document",
  "Validate",
  "Publish",
  "Archive",
  "Merge",
  "Revert",
  "Ship",
];

function generateTitle(index: number): string {
  const verb = VERBS[index % VERBS.length];
  const adj = ADJECTIVES[Math.floor(index / VERBS.length) % ADJECTIVES.length];
  const noun = NOUNS[Math.floor(index / (VERBS.length * ADJECTIVES.length)) % NOUNS.length];
  return `${verb} ${adj} ${noun} #${index + 1}`;
}

function generateDescription(index: number): string | undefined {
  if (index % 3 === 0) return undefined;
  return `Auto-generated stress test item ${index + 1}. Priority: ${index % 5 === 0 ? "high" : index % 3 === 0 ? "medium" : "low"}.`;
}

type LogEntry = {
  timestamp: number;
  message: string;
};

export function StressTest() {
  const db = useDb();
  const session = useSession();
  const sessionUserId = session?.user_id ?? null;
  const [subscribed, setSubscribed] = useState(false);
  const todos = useAll(subscribed ? app.todos : undefined) ?? [];

  const [count, setCount] = useState("15000");
  const [isRunning, setIsRunning] = useState(false);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [progress, setProgress] = useState({ inserted: 0, total: 0 });
  const abortRef = useRef(false);

  const log = useCallback((message: string) => {
    setLogs((prev) => [...prev, { timestamp: Date.now(), message }]);
  }, []);

  const generateTodos = useCallback(async () => {
    if (!sessionUserId) {
      log("No session user ID available");
      return;
    }

    const total = parseInt(count, 10);
    if (isNaN(total) || total <= 0) {
      log("Invalid count");
      return;
    }

    setIsRunning(true);
    abortRef.current = false;
    setProgress({ inserted: 0, total });
    setLogs([]);

    log(`Starting generation of ${total} todos (+ ${PROJECT_COUNT} projects)...`);
    const startTime = performance.now();

    // Generate projects first so we can assign them to todos
    const projectIds: string[] = [];
    for (let i = 0; i < PROJECT_COUNT; i++) {
      const row = db.insert(app.projects, {
        name: PROJECT_NAMES[i % PROJECT_NAMES.length],
      });
      projectIds.push(row.id);
    }
    log(`Created ${PROJECT_COUNT} projects`);

    let inserted = 0;
    const batches = Math.ceil(total / BATCH_SIZE);

    for (let batch = 0; batch < batches; batch++) {
      if (abortRef.current) {
        log(`Aborted after ${inserted} inserts`);
        break;
      }

      const batchStart = batch * BATCH_SIZE;
      const batchEnd = Math.min(batchStart + BATCH_SIZE, total);
      const batchStartTime = performance.now();

      for (let i = batchStart; i < batchEnd; i++) {
        db.insert(app.todos, {
          title: generateTitle(i),
          done: i % 7 === 0,
          description: generateDescription(i),
          owner_id: sessionUserId,
          // Assign ~70% of todos to a project
          project_id: i % 10 < 7 ? projectIds[i % projectIds.length] : undefined,
        });
        inserted++;
      }

      const batchElapsed = performance.now() - batchStartTime;
      setProgress({ inserted, total });
      log(
        `Batch ${batch + 1}/${batches}: inserted ${batchEnd - batchStart} todos in ${batchElapsed.toFixed(0)}ms (${inserted}/${total} total)`,
      );

      // Yield to the UI thread between batches
      await new Promise((resolve) => setTimeout(resolve, 0));
    }

    const elapsed = performance.now() - startTime;
    const rate = (inserted / elapsed) * 1000;
    log(
      `Done: ${inserted} todos in ${(elapsed / 1000).toFixed(2)}s (${rate.toFixed(0)} inserts/sec)`,
    );
    setIsRunning(false);
  }, [count, sessionUserId, db, log]);

  const deleteAllTodos = useCallback(async () => {
    if (todos.length === 0) {
      log("No todos to delete");
      return;
    }

    setIsRunning(true);
    abortRef.current = false;
    const total = todos.length;
    setProgress({ inserted: 0, total });
    log(`Deleting ${total} todos...`);
    const startTime = performance.now();

    let deleted = 0;
    const batches = Math.ceil(total / BATCH_SIZE);
    const todoIds = todos.map((t) => t.id);

    for (let batch = 0; batch < batches; batch++) {
      if (abortRef.current) {
        log(`Aborted after ${deleted} deletes`);
        break;
      }

      const batchStart = batch * BATCH_SIZE;
      const batchEnd = Math.min(batchStart + BATCH_SIZE, total);

      for (let i = batchStart; i < batchEnd; i++) {
        db.delete(app.todos, todoIds[i]);
        deleted++;
      }

      setProgress({ inserted: deleted, total });

      await new Promise((resolve) => setTimeout(resolve, 0));
    }

    const elapsed = performance.now() - startTime;
    log(`Deleted ${deleted} todos in ${(elapsed / 1000).toFixed(2)}s`);
    setIsRunning(false);
  }, [todos, db, log]);

  const abort = useCallback(() => {
    abortRef.current = true;
    log("Abort requested...");
  }, [log]);

  const pct = progress.total > 0 ? Math.round((progress.inserted / progress.total) * 100) : 0;

  return (
    <View style={styles.wrapper}>
      {/* Stats */}
      <View style={styles.statsRow}>
        <View style={styles.statBox}>
          <Text style={styles.statValue}>{todos.length.toLocaleString()}</Text>
          <Text style={styles.statLabel}>Total Todos</Text>
        </View>
        <View style={styles.statBox}>
          <Text style={styles.statValue}>
            {todos.filter((t) => t.done).length.toLocaleString()}
          </Text>
          <Text style={styles.statLabel}>Done</Text>
        </View>
        <View style={styles.statBox}>
          <Text style={styles.statValue}>
            {todos.filter((t) => !t.done).length.toLocaleString()}
          </Text>
          <Text style={styles.statLabel}>Pending</Text>
        </View>
      </View>

      {/* Controls */}
      <View style={styles.controlsRow}>
        <TextInput
          style={styles.countInput}
          value={count}
          onChangeText={setCount}
          keyboardType="number-pad"
          editable={!isRunning}
          placeholder="Count"
        />
        <Pressable
          style={[styles.button, styles.generateButton, isRunning && styles.buttonDisabled]}
          onPress={generateTodos}
          disabled={isRunning || !sessionUserId}
        >
          <Text style={styles.buttonText}>Generate</Text>
        </Pressable>
        <Pressable
          style={[styles.button, styles.deleteButton, isRunning && styles.buttonDisabled]}
          onPress={deleteAllTodos}
          disabled={isRunning || todos.length === 0}
        >
          <Text style={styles.buttonText}>Delete All</Text>
        </Pressable>
      </View>

      {/* Subscription toggle */}
      <Pressable
        style={[
          styles.button,
          subscribed ? styles.subscribeActiveButton : styles.subscribeInactiveButton,
        ]}
        onPress={() => setSubscribed((v) => !v)}
      >
        <Text style={styles.buttonText}>Subscription: {subscribed ? "ON" : "OFF"}</Text>
      </Pressable>

      {isRunning && (
        <View style={styles.progressSection}>
          <View style={styles.progressRow}>
            <View style={styles.progressBarBg}>
              <View style={[styles.progressBarFill, { width: `${pct}%` }]} />
            </View>
            <Pressable style={[styles.button, styles.abortButton]} onPress={abort}>
              <Text style={styles.buttonText}>Stop</Text>
            </Pressable>
          </View>
          <Text style={styles.progressText}>
            {progress.inserted.toLocaleString()} / {progress.total.toLocaleString()} ({pct}%)
          </Text>
        </View>
      )}

      {/* Logs */}
      <ScrollView style={styles.logContainer}>
        {logs.map((entry, i) => (
          <Text key={i} style={styles.logLine}>
            {entry.message}
          </Text>
        ))}
        {logs.length === 0 && (
          <Text style={styles.logPlaceholder}>
            Enter a count and press Generate to insert todos in bulk.
          </Text>
        )}
      </ScrollView>
    </View>
  );
}

const styles = StyleSheet.create({
  wrapper: {
    flex: 1,
    gap: 12,
  },
  statsRow: {
    flexDirection: "row",
    gap: 8,
  },
  statBox: {
    flex: 1,
    backgroundColor: "#fff",
    borderRadius: 10,
    borderWidth: 1,
    borderColor: "#e5e7eb",
    padding: 12,
    alignItems: "center",
  },
  statValue: {
    fontSize: 20,
    fontWeight: "700",
    color: "#111827",
  },
  statLabel: {
    fontSize: 12,
    color: "#6b7280",
    marginTop: 2,
  },
  controlsRow: {
    flexDirection: "row",
    gap: 8,
  },
  countInput: {
    flex: 1,
    borderWidth: 1,
    borderColor: "#d1d5db",
    borderRadius: 10,
    backgroundColor: "#fff",
    paddingHorizontal: 12,
    paddingVertical: 10,
    fontSize: 16,
    fontWeight: "600",
  },
  button: {
    alignItems: "center",
    justifyContent: "center",
    paddingHorizontal: 14,
    paddingVertical: 10,
    borderRadius: 10,
  },
  buttonDisabled: {
    opacity: 0.4,
  },
  generateButton: {
    backgroundColor: "#059669",
  },
  deleteButton: {
    backgroundColor: "#b91c1c",
  },
  abortButton: {
    backgroundColor: "#d97706",
    paddingHorizontal: 12,
  },
  subscribeActiveButton: {
    backgroundColor: "#2563eb",
  },
  subscribeInactiveButton: {
    backgroundColor: "#6b7280",
  },
  buttonText: {
    color: "#fff",
    fontWeight: "600",
    fontSize: 14,
  },
  progressSection: {
    gap: 4,
  },
  progressRow: {
    flexDirection: "row",
    gap: 8,
    alignItems: "center",
  },
  progressBarBg: {
    flex: 1,
    height: 8,
    backgroundColor: "#e5e7eb",
    borderRadius: 4,
    overflow: "hidden",
  },
  progressBarFill: {
    height: "100%",
    backgroundColor: "#059669",
    borderRadius: 4,
  },
  progressText: {
    fontSize: 12,
    color: "#6b7280",
  },
  logContainer: {
    flex: 1,
    backgroundColor: "#1f2937",
    borderRadius: 10,
    padding: 12,
  },
  logLine: {
    color: "#d1fae5",
    fontSize: 12,
    fontFamily: "monospace",
    lineHeight: 18,
  },
  logPlaceholder: {
    color: "#6b7280",
    fontSize: 13,
    fontStyle: "italic",
  },
});
