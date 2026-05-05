import { useState } from "react";
import { FlatList, Pressable, StyleSheet, Switch, Text, TextInput, View } from "react-native";
import { useDb, useAll } from "jazz-tools/react-native";
import { app } from "../schema";

export function TodoWidget() {
  const db = useDb();
  const todos = useAll(app.todos) ?? [];
  const [title, setTitle] = useState("");

  function add() {
    const trimmed = title.trim();
    if (!trimmed) return;
    db.insert(app.todos, { title: trimmed, done: false });
    setTitle("");
  }

  return (
    <View style={styles.section}>
      <Text style={styles.heading}>Your todos</Text>
      <View style={styles.inputRow}>
        <TextInput
          value={title}
          onChangeText={setTitle}
          placeholder="Add a task"
          style={styles.input}
          returnKeyType="done"
          onSubmitEditing={add}
          accessibilityLabel="New todo"
        />
        <Pressable
          onPress={add}
          style={styles.addButton}
          accessibilityLabel="Add todo"
          accessibilityRole="button"
        >
          <Text style={styles.addButtonText}>Add</Text>
        </Pressable>
      </View>
      <FlatList
        data={todos}
        scrollEnabled={false}
        keyExtractor={(item) => item.id}
        ItemSeparatorComponent={() => <View style={styles.separator} />}
        ListEmptyComponent={<Text style={styles.empty}>No todos yet.</Text>}
        renderItem={({ item }) => (
          <View style={styles.row}>
            <Switch
              value={item.done}
              onValueChange={() => {
                db.update(app.todos, item.id, { done: !item.done });
              }}
            />
            <Text style={[styles.rowTitle, item.done && styles.rowDone]}>{item.title}</Text>
            <Pressable
              onPress={() => db.delete(app.todos, item.id)}
              style={styles.deleteButton}
              accessibilityLabel="Delete"
            >
              <Text style={styles.deleteText}>×</Text>
            </Pressable>
          </View>
        )}
      />
    </View>
  );
}

const styles = StyleSheet.create({
  section: {
    gap: 12,
    backgroundColor: "#fff",
    borderRadius: 12,
    padding: 16,
    borderWidth: 1,
    borderColor: "#e5e7eb",
  },
  heading: {
    fontSize: 18,
    fontWeight: "600",
    color: "#111827",
  },
  inputRow: {
    flexDirection: "row",
    gap: 8,
  },
  input: {
    flex: 1,
    borderWidth: 1,
    borderColor: "#d1d5db",
    borderRadius: 10,
    backgroundColor: "#fff",
    paddingHorizontal: 12,
    paddingVertical: 10,
  },
  addButton: {
    alignItems: "center",
    justifyContent: "center",
    paddingHorizontal: 14,
    borderRadius: 10,
    backgroundColor: "#111827",
  },
  addButtonText: {
    color: "#fff",
    fontWeight: "600",
  },
  separator: {
    height: 8,
  },
  empty: {
    color: "#6b7280",
    fontSize: 14,
  },
  row: {
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
  rowTitle: {
    flex: 1,
    color: "#111827",
    fontSize: 16,
  },
  rowDone: {
    textDecorationLine: "line-through",
    color: "#6b7280",
  },
  deleteButton: {
    paddingHorizontal: 10,
    paddingVertical: 4,
  },
  deleteText: {
    color: "#b91c1c",
    fontSize: 20,
    fontWeight: "600",
  },
});
