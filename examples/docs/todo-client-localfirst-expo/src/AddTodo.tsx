import { useState } from "react";
import { Pressable, Text, TextInput, View } from "react-native";
import { useDb } from "jazz-tools/react-native";
import { app } from "../schema/app";

export function AddTodo() {
  const db = useDb();
  const [title, setTitle] = useState("");

  const handleAdd = () => {
    const trimmed = title.trim();
    if (!trimmed) return;
    db.insert(app.todos, { title: trimmed, done: false, ownerId: "" });
    setTitle("");
  };

  return (
    <View style={{ flexDirection: "row", gap: 8 }}>
      <TextInput
        value={title}
        onChangeText={setTitle}
        placeholder="New todo"
        style={{ flex: 1, borderWidth: 1, borderColor: "#ccc", padding: 8, borderRadius: 8 }}
      />
      <Pressable onPress={handleAdd}>
        <Text>Add</Text>
      </Pressable>
    </View>
  );
}
