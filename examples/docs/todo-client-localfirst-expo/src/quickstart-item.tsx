import { Pressable, Switch, Text, View } from "react-native";
import { useDb, useAll } from "jazz-tools/react-native";
import { app } from "../schema";

export function TodoItem({ id }: { id: string }) {
  const db = useDb();
  const [todo] = useAll(app.todos.where({ id }).limit(1)) ?? [];

  if (!todo) return null;

  return (
    <View>
      <Switch
        value={todo.done}
        onValueChange={() => db.update(app.todos, id, { done: !todo.done })}
      />
      <Text>{todo.title}</Text>
      <Pressable onPress={() => db.delete(app.todos, id)}>
        <Text>Delete</Text>
      </Pressable>
    </View>
  );
}
