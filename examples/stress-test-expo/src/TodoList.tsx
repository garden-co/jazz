import { useState } from "react";
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
import { app, type Todo } from "../schema/app";

function normalizeText(value: string | null | undefined): string {
  return typeof value === "string" ? value : "";
}

export function TodoList() {
  const [filterTitle, setFilterTitle] = useState("");
  const [showDoneOnly, setShowDoneOnly] = useState(false);

  const trimmedFilterTitle = normalizeText(filterTitle).trim();
  let todosQuery = app.todos;
  if (trimmedFilterTitle) {
    todosQuery = todosQuery.where({ title: { contains: trimmedFilterTitle } });
  }
  if (showDoneOnly) {
    todosQuery = todosQuery.where({ done: true });
  }

  const db = useDb();
  const todos = useAll(todosQuery) ?? [];
  const session = useSession();
  const sessionUserId = session?.user_id ?? null;

  const renderItem: ListRenderItem<Todo> = ({ item }) => {
    const displayTitle = normalizeText(item.title).trim() || "Untitled todo";

    return (
      <View style={styles.todoRow}>
        <Switch
          value={item.done}
          onValueChange={() => db.update(app.todos, item.id, { done: !item.done })}
        />
        <View style={styles.todoTextWrap}>
          <Text style={[styles.todoTitle, item.done && styles.todoDone]}>{displayTitle}</Text>
          {item.description ? <Text style={styles.todoDescription}>{item.description}</Text> : null}
        </View>
        <Pressable onPress={() => db.delete(app.todos, item.id)} style={styles.deleteButton}>
          <Text style={styles.deleteButtonText}>Delete</Text>
        </Pressable>
      </View>
    );
  };

  return (
    <View style={styles.wrapper}>
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

      <Text style={styles.countText}>{todos.length} todos loaded</Text>

      <FlatList
        data={todos}
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
  countText: {
    color: "#6b7280",
    fontSize: 13,
    fontWeight: "500",
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
