import React, { useState, useCallback, useEffect, useRef } from "react";
import { FlatList, StyleSheet, Text, TextInput, TouchableOpacity, View } from "react-native";
import type { JazzRuntime } from "./jazz-runtime";
import { todoValues, todoUpdate, parseDelta, type Todo } from "./schema";

interface Props {
  runtime: JazzRuntime;
}

export function TodoTab({ runtime }: Props) {
  const [todos, setTodos] = useState<Todo[]>([]);
  const [input, setInput] = useState("");
  const handleRef = useRef<number | null>(null);

  // Subscribe to all todos on mount
  useEffect(() => {
    const queryJson = JSON.stringify({ table: "todos" });

    const handle = runtime.subscribe(
      queryJson,
      (deltaJson: string) => {
        const delta = parseDelta(deltaJson);

        setTodos((prev) => {
          let next = [...prev];

          // Remove
          for (const r of delta.removed) {
            next = next.filter((t) => t.id !== r.row.id);
          }

          // Add
          for (const a of delta.added) {
            // Avoid duplicates (initial snapshot may overlap)
            if (!next.some((t) => t.id === a.row.id)) {
              next.splice(a.index, 0, a.row);
            }
          }

          // Update
          for (const u of delta.updated) {
            const idx = next.findIndex((t) => t.id === u.oldRow.id);
            if (idx !== -1) {
              next[idx] = u.newRow;
            }
          }

          return next;
        });
      },
      undefined,
      undefined,
    );

    handleRef.current = handle;

    // Tick to fire initial snapshot
    runtime.batchedTick();

    return () => {
      if (handleRef.current !== null) {
        runtime.unsubscribe(handleRef.current);
        handleRef.current = null;
      }
    };
  }, [runtime]);

  const handleAdd = useCallback(() => {
    const title = input.trim();
    if (!title) return;
    runtime.insert("todos", todoValues(title, false));
    runtime.batchedTick();
    setInput("");
  }, [input, runtime]);

  const handleToggle = useCallback(
    (todo: Todo) => {
      runtime.update(todo.id, todoUpdate({ done: !todo.done }));
      runtime.batchedTick();
    },
    [runtime],
  );

  const handleDelete = useCallback(
    (todo: Todo) => {
      runtime.deleteRow(todo.id);
      runtime.batchedTick();
    },
    [runtime],
  );

  const handleClearAll = useCallback(() => {
    for (const todo of todos) {
      runtime.deleteRow(todo.id);
    }
    runtime.batchedTick();
  }, [todos, runtime]);

  const renderItem = useCallback(
    ({ item }: { item: Todo }) => (
      <View style={styles.todoRow}>
        <TouchableOpacity style={styles.todoCheck} onPress={() => handleToggle(item)}>
          <Text style={styles.todoCheckText}>{item.done ? "[x]" : "[ ]"}</Text>
        </TouchableOpacity>
        <Text style={[styles.todoTitle, item.done && styles.todoTitleDone]} numberOfLines={1}>
          {item.title}
        </Text>
        <TouchableOpacity style={styles.todoDelete} onPress={() => handleDelete(item)}>
          <Text style={styles.todoDeleteText}>X</Text>
        </TouchableOpacity>
      </View>
    ),
    [handleToggle, handleDelete],
  );

  return (
    <View style={styles.container}>
      <View style={styles.inputRow}>
        <TextInput
          style={styles.input}
          value={input}
          onChangeText={setInput}
          placeholder="Add a todo..."
          placeholderTextColor="#666"
          onSubmitEditing={handleAdd}
          returnKeyType="done"
        />
        <TouchableOpacity style={styles.addBtn} onPress={handleAdd}>
          <Text style={styles.addBtnText}>Add</Text>
        </TouchableOpacity>
      </View>

      <FlatList
        data={todos}
        keyExtractor={(item) => item.id}
        renderItem={renderItem}
        style={styles.list}
        contentContainerStyle={styles.listContent}
        ListEmptyComponent={<Text style={styles.emptyText}>No todos yet. Add one above.</Text>}
      />

      <Text style={styles.countText}>
        {todos.length} todo{todos.length !== 1 ? "s" : ""}
        {" | "}
        {todos.filter((t) => t.done).length} done
      </Text>

      {todos.length > 0 && (
        <TouchableOpacity style={styles.clearBtn} onPress={handleClearAll}>
          <Text style={styles.clearBtnText}>Clear All</Text>
        </TouchableOpacity>
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    gap: 8,
  },
  inputRow: {
    flexDirection: "row",
    gap: 8,
  },
  input: {
    flex: 1,
    backgroundColor: "#1a1a1a",
    borderRadius: 6,
    paddingHorizontal: 12,
    paddingVertical: 10,
    color: "#fff",
    fontSize: 14,
    borderWidth: 1,
    borderColor: "#333",
  },
  addBtn: {
    backgroundColor: "#2563eb",
    borderRadius: 6,
    paddingHorizontal: 16,
    justifyContent: "center",
  },
  addBtnText: {
    color: "#fff",
    fontSize: 14,
    fontWeight: "600",
  },
  list: {
    flex: 1,
    backgroundColor: "#111",
    borderRadius: 6,
  },
  listContent: {
    padding: 4,
  },
  todoRow: {
    flexDirection: "row",
    alignItems: "center",
    paddingVertical: 10,
    paddingHorizontal: 8,
    borderBottomWidth: 1,
    borderBottomColor: "#222",
  },
  todoCheck: {
    marginRight: 8,
  },
  todoCheckText: {
    color: "#8b8",
    fontSize: 14,
    fontFamily: "monospace",
  },
  todoTitle: {
    flex: 1,
    color: "#eee",
    fontSize: 14,
  },
  todoTitleDone: {
    textDecorationLine: "line-through",
    color: "#666",
  },
  todoDelete: {
    marginLeft: 8,
    paddingHorizontal: 8,
    paddingVertical: 4,
  },
  todoDeleteText: {
    color: "#e55",
    fontSize: 12,
    fontWeight: "700",
  },
  countText: {
    color: "#666",
    fontSize: 11,
    textAlign: "center",
  },
  emptyText: {
    color: "#555",
    fontSize: 13,
    textAlign: "center",
    paddingVertical: 24,
  },
  clearBtn: {
    backgroundColor: "#7f1d1d",
    borderRadius: 6,
    paddingVertical: 10,
    alignItems: "center",
  },
  clearBtnText: {
    color: "#fca5a5",
    fontSize: 13,
    fontWeight: "600",
  },
});
