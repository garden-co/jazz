import { useState } from "react";
import { Pressable, Text, TextInput, View } from "react-native";
import { useDb } from "jazz-tools/react-native";
import { app } from "../schema/app";

export function AddTodo() {
  const db = useDb();
  const [title, setTitle] = useState("");

  return (
    <View>
      <TextInput
        value={title}
        onChangeText={setTitle}
        placeholder="What needs to be done?"
        onSubmitEditing={() => {
          db.insert(app.todos, { title, done: false });
          setTitle("");
        }}
      />
      <Pressable
        onPress={() => {
          db.insert(app.todos, { title, done: false });
          setTitle("");
        }}
      >
        <Text>Add</Text>
      </Pressable>
    </View>
  );
}
