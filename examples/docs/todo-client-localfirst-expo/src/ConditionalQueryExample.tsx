import { useState } from "react";
import { Text, TextInput, View } from "react-native";
import { useAll } from "jazz-tools/react-native";
import { app } from "../schema/app";

// #region reading-conditional-query-expo
export function FilteredTodos() {
  const [filter, setFilter] = useState<string | null>(null);
  const filtered = useAll(filter ? app.todos.where({ title: { contains: filter } }) : undefined);

  return (
    <View>
      <TextInput
        value={filter ?? ""}
        onChangeText={(v) => setFilter(v || null)}
        placeholder="Filter by title"
      />
      {filtered?.map((todo) => (
        <Text key={todo.id}>{todo.title}</Text>
      ))}
    </View>
  );
}
// #endregion reading-conditional-query-expo
