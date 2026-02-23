import { useEffect, useState } from "react";
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
import { resolveClientSession, type LocalAuthMode } from "jazz-tools";
import { useAll, useDb } from "jazz-tools/react-native";
import { app, type Todo } from "../schema/app";

interface TodoListProps {
  appId: string;
  localAuthMode?: LocalAuthMode;
  localAuthToken?: string;
  jwtToken?: string;
}

export function TodoList({ appId, localAuthMode, localAuthToken, jwtToken }: TodoListProps) {
  const db = useDb();
  const todos = useAll(app.todos);
  const [title, setTitle] = useState("");
  const [sessionUserId, setSessionUserId] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    resolveClientSession({ appId, localAuthMode, localAuthToken, jwtToken })
      .then((session) => {
        if (!cancelled) {
          setSessionUserId(session?.user_id ?? null);
        }
      })
      .catch((error) => {
        // Keep a visible signal for local session derivation failures on device.
        console.warn("[todo-client-localfirst-expo] failed to resolve local session", error);
        if (!cancelled) {
          setSessionUserId(null);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [appId, localAuthMode, localAuthToken, jwtToken]);

  const addTodo = () => {
    const trimmed = title.trim();
    if (!trimmed || !sessionUserId) return;
    db.insert(app.todos, { title: trimmed, done: false, owner_id: sessionUserId });
    setTitle("");
  };

  const renderItem: ListRenderItem<Todo> = ({ item }) => {
    const canEdit = sessionUserId === item.owner_id;

    return (
      <View style={styles.todoRow}>
        <Switch
          value={item.done}
          disabled={!canEdit}
          onValueChange={() => db.update(app.todos, item.id, { done: !item.done })}
        />
        <View style={styles.todoTextWrap}>
          <Text style={[styles.todoTitle, item.done && styles.todoDone]}>{item.title}</Text>
          {item.description ? <Text style={styles.todoDescription}>{item.description}</Text> : null}
        </View>
        <Pressable
          disabled={!canEdit}
          onPress={() => db.deleteFrom(app.todos, item.id)}
          style={styles.deleteButton}
        >
          <Text style={[styles.deleteButtonText, !canEdit && styles.disabledText]}>Delete</Text>
        </Pressable>
      </View>
    );
  };

  return (
    <View style={styles.wrapper}>
      <View style={styles.inputRow}>
        <TextInput
          value={title}
          onChangeText={setTitle}
          placeholder="What needs to be done?"
          style={styles.input}
          editable={!!sessionUserId}
          returnKeyType="done"
          onSubmitEditing={addTodo}
        />
        <Pressable
          disabled={!sessionUserId}
          onPress={addTodo}
          style={[styles.addButton, !sessionUserId && styles.disabledButton]}
        >
          <Text style={styles.addButtonText}>Add</Text>
        </Pressable>
      </View>
      {!sessionUserId ? (
        <Text style={styles.emptyText}>Waiting for local auth session...</Text>
      ) : null}

      <FlatList
        data={todos}
        renderItem={renderItem}
        keyExtractor={(item) => item.id}
        ItemSeparatorComponent={() => <View style={styles.separator} />}
        ListEmptyComponent={<Text style={styles.emptyText}>No todos yet.</Text>}
      />
    </View>
  );
}

const styles = StyleSheet.create({
  wrapper: {
    flex: 1,
    gap: 12,
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
  disabledButton: {
    opacity: 0.5,
  },
  addButtonText: {
    color: "#fff",
    fontWeight: "600",
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
  disabledText: {
    opacity: 0.4,
  },
});
