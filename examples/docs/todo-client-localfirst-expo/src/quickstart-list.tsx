import { FlatList, View } from "react-native";
import { useAll } from "jazz-tools/react-native";
import { app } from "../schema/app";
import { TodoItem } from "./TodoItem";
import { AddTodo } from "./AddTodo";

export function TodoList() {
  const todos = useAll(app.todos) ?? [];

  return (
    <View style={{ flex: 1, gap: 12 }}>
      <FlatList
        data={todos}
        keyExtractor={(item) => item.id}
        renderItem={({ item }) => <TodoItem id={item.id} />}
      />
      <AddTodo />
    </View>
  );
}
