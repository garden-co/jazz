import { Pressable, Text, View } from "react-native";
import { useDb, useAll } from "jazz-tools/react-native";
import { app } from "../schema";

export function TodoItem({ id }: { id: string }) {
  const db = useDb();
  const [todo] = useAll(app.todos.where({ id }).limit(1)) ?? [];

  if (!todo) return null;

  return (
    <View style={{ flexDirection: "row", alignItems: "center", gap: 8, paddingVertical: 4 }}>
      <Pressable onPress={() => db.update(app.todos, id, { done: !todo.done })}>
        <Text>{todo.done ? "☑" : "☐"}</Text>
      </Pressable>
      <Text style={{ flex: 1, textDecorationLine: todo.done ? "line-through" : "none" }}>
        {todo.title}
      </Text>
      <Pressable onPress={() => db.delete(app.todos, id)}>
        <Text>✕</Text>
      </Pressable>
    </View>
  );
}
