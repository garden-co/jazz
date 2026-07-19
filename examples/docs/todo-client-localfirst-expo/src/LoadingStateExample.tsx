import { Text } from "react-native";
import { useAll } from "jazz-tools/react";
import { app } from "../schema";

// #region reading-loading-state-expo
export function TodoList() {
  const todos = useAll(app.todos);

  if (!todos) {
    return <Text>Connecting…</Text>;
  }
  // Empty array means no rows, not "still loading".

  return todos.map((todo) => <Text key={todo.id}>{todo.title}</Text>);
}
// #endregion reading-loading-state-expo
