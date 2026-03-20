import { Text } from "react-native";
import { useAll } from "jazz-tools/react-native";
import { app } from "../schema/app";

// #region reading-loading-state-expo
export function TodoList() {
  const todos = useAll(app.todos);

  if (todos === undefined) {
    return <Text>Connecting…</Text>;
  }
  // todos is now Todo[] — empty array means no rows, not "still loading"

  return todos.map((todo) => <Text key={todo.id}>{todo.title}</Text>);
}
// #endregion reading-loading-state-expo
