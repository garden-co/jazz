import { useState } from "react";
import { Pressable, Text, TextInput, View } from "react-native";
import { useDb, useSession } from "jazz-tools/react-native";
import { app } from "../schema";

export function AddTodo() {
  const db = useDb();
  const session = useSession();
  const [title, setTitle] = useState("");

  const addTodo = () => {
    if (!title.trim() || !session) return;
    db.insert(app.todos, { title, done: false, ownerId: session.user_id });
    setTitle("");
  };

  return (
    <View>
      <TextInput
        value={title}
        onChangeText={setTitle}
        placeholder="What needs to be done?"
        onSubmitEditing={addTodo}
      />
      <Pressable onPress={addTodo}>
        <Text>Add</Text>
      </Pressable>
    </View>
  );
}
