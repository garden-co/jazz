import { useEffect, useRef, useState } from "react";
import {
  FlatList,
  Pressable,
  StyleSheet,
  Switch,
  Text,
  TextInput,
  View,
  type ListRenderItem,
} from "react-native";
import { useAll, useDb, useSession } from "jazz-tools/react-native";
import { app, type Todo, type TodoWithIncludes } from "../schema/app";

function normalizeText(value: string | null | undefined): string {
  return typeof value === "string" ? value : "";
}

type QueryMode = "plain" | "join_project" | "join_parent" | "join_both" | "filtered_join";

const QUERY_MODES: { key: QueryMode; label: string; description: string }[] = [
  { key: "plain", label: "Plain", description: "SELECT * FROM todos" },
  { key: "join_project", label: "Join Project", description: "todos + include(project)" },
  { key: "join_parent", label: "Join Parent", description: "todos + include(parent)" },
  { key: "join_both", label: "Join Both", description: "todos + include(project, parent)" },
  { key: "filtered_join", label: "Filter + Join", description: "where(done) + include(project)" },
];

function buildQuery(mode: QueryMode, filterTitle: string, showDoneOnly: boolean) {
  let query: any = app.todos;

  const trimmed = filterTitle.trim();
  if (trimmed) {
    query = query.where({ title: { contains: trimmed } });
  }
  if (showDoneOnly) {
    query = query.where({ done: true });
  }

  switch (mode) {
    case "join_project":
      query = query.include({ project: true });
      break;
    case "join_parent":
      query = query.include({ parent: true });
      break;
    case "join_both":
      query = query.include({ project: true, parent: true });
      break;
    case "filtered_join":
      query = query.where({ done: true }).include({ project: true });
      break;
  }

  return query;
}

export function TodoList() {
  const [filterTitle, setFilterTitle] = useState("");
  const [showDoneOnly, setShowDoneOnly] = useState(false);
  const [queryMode, setQueryMode] = useState<QueryMode>("plain");
  const [queryTimeMs, setQueryTimeMs] = useState<number | null>(null);
  const queryStartRef = useRef<number>(0);
  const prevResultLenRef = useRef<number>(-1);

  const query = buildQuery(queryMode, normalizeText(filterTitle), showDoneOnly);

  // Track when query changes to measure time-to-first-result
  useEffect(() => {
    queryStartRef.current = performance.now();
    prevResultLenRef.current = -1;
    setQueryTimeMs(null);
  }, [queryMode, filterTitle, showDoneOnly]);

  const results = useAll(query) ?? [];

  // Measure time when results first arrive after a query change
  useEffect(() => {
    if (prevResultLenRef.current === -1 && results.length > 0) {
      const elapsed = performance.now() - queryStartRef.current;
      setQueryTimeMs(elapsed);
    }
    prevResultLenRef.current = results.length;
  }, [results.length]);

  const db = useDb();

  const renderItem: ListRenderItem<any> = ({ item }) => {
    const displayTitle = normalizeText(item.title).trim() || "Untitled todo";
    const projectName = item.project?.name;
    const parentTitle = item.parent?.title;

    return (
      <View style={styles.todoRow}>
        <Switch
          value={item.done}
          onValueChange={() => db.update(app.todos, item.id, { done: !item.done })}
        />
        <View style={styles.todoTextWrap}>
          <Text style={[styles.todoTitle, item.done && styles.todoDone]}>{displayTitle}</Text>
          {item.description ? <Text style={styles.todoDescription}>{item.description}</Text> : null}
          {projectName ? <Text style={styles.relationTag}>Project: {projectName}</Text> : null}
          {parentTitle ? <Text style={styles.relationTag}>Parent: {parentTitle}</Text> : null}
        </View>
        <Pressable onPress={() => db.delete(app.todos, item.id)} style={styles.deleteButton}>
          <Text style={styles.deleteButtonText}>Delete</Text>
        </Pressable>
      </View>
    );
  };

  return (
    <View style={styles.wrapper}>
      {/* Query mode selector */}
      <View style={styles.queryModeSection}>
        <Text style={styles.sectionLabel}>Query Mode</Text>
        <View style={styles.queryModeRow}>
          {QUERY_MODES.map((m) => (
            <Pressable
              key={m.key}
              style={[styles.queryModeChip, queryMode === m.key && styles.queryModeChipActive]}
              onPress={() => setQueryMode(m.key)}
            >
              <Text
                style={[
                  styles.queryModeChipText,
                  queryMode === m.key && styles.queryModeChipTextActive,
                ]}
              >
                {m.label}
              </Text>
            </Pressable>
          ))}
        </View>
        <Text style={styles.queryDescription}>
          {QUERY_MODES.find((m) => m.key === queryMode)?.description}
        </Text>
      </View>

      {/* Filters */}
      <View style={styles.filters}>
        <TextInput
          value={normalizeText(filterTitle)}
          onChangeText={setFilterTitle}
          placeholder="Filter by title (contains)"
          style={styles.filterInput}
        />
        <View style={styles.doneOnlyRow}>
          <Text style={styles.doneOnlyLabel}>Done only</Text>
          <Switch value={showDoneOnly} onValueChange={setShowDoneOnly} />
        </View>
      </View>

      {/* Stats */}
      <View style={styles.statsRow}>
        <Text style={styles.countText}>{results.length} todos loaded</Text>
        {queryTimeMs !== null && (
          <Text style={styles.timingText}>{queryTimeMs.toFixed(0)}ms to first result</Text>
        )}
      </View>

      <FlatList
        data={results}
        renderItem={renderItem}
        keyExtractor={(item) => item.id}
        ItemSeparatorComponent={() => <View style={styles.separator} />}
        ListEmptyComponent={<Text style={styles.emptyText}>No todos yet.</Text>}
        initialNumToRender={20}
        maxToRenderPerBatch={30}
        windowSize={5}
      />
    </View>
  );
}

const styles = StyleSheet.create({
  wrapper: {
    flex: 1,
    gap: 12,
  },
  sectionLabel: {
    fontSize: 13,
    fontWeight: "600",
    color: "#374151",
    marginBottom: 6,
  },
  queryModeSection: {
    gap: 4,
  },
  queryModeRow: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 6,
  },
  queryModeChip: {
    paddingHorizontal: 10,
    paddingVertical: 6,
    borderRadius: 8,
    backgroundColor: "#e5e7eb",
  },
  queryModeChipActive: {
    backgroundColor: "#2563eb",
  },
  queryModeChipText: {
    fontSize: 12,
    fontWeight: "600",
    color: "#374151",
  },
  queryModeChipTextActive: {
    color: "#fff",
  },
  queryDescription: {
    fontSize: 11,
    color: "#6b7280",
    fontFamily: "monospace",
    marginTop: 2,
  },
  filters: {
    gap: 8,
  },
  filterInput: {
    borderWidth: 1,
    borderColor: "#d1d5db",
    borderRadius: 10,
    backgroundColor: "#fff",
    paddingHorizontal: 12,
    paddingVertical: 10,
  },
  doneOnlyRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  doneOnlyLabel: {
    color: "#374151",
    fontSize: 14,
  },
  statsRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
  },
  countText: {
    color: "#6b7280",
    fontSize: 13,
    fontWeight: "500",
  },
  timingText: {
    color: "#059669",
    fontSize: 13,
    fontWeight: "600",
    fontFamily: "monospace",
  },
  separator: {
    height: 8,
  },
  emptyText: {
    color: "#6b7280",
    fontSize: 14,
  },
  todoRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 10,
    borderWidth: 1,
    borderColor: "#e5e7eb",
    borderRadius: 10,
    backgroundColor: "#fff",
    paddingHorizontal: 12,
    paddingVertical: 10,
  },
  todoTextWrap: {
    flex: 1,
    gap: 2,
  },
  todoTitle: {
    color: "#111827",
    fontSize: 16,
    fontWeight: "500",
  },
  todoDone: {
    textDecorationLine: "line-through",
    color: "#6b7280",
  },
  todoDescription: {
    color: "#4b5563",
    fontSize: 13,
  },
  relationTag: {
    fontSize: 11,
    color: "#2563eb",
    fontWeight: "500",
  },
  deleteButton: {
    paddingHorizontal: 8,
    paddingVertical: 4,
  },
  deleteButtonText: {
    color: "#b91c1c",
    fontSize: 13,
    fontWeight: "600",
  },
});
