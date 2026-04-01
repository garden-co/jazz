import { Text, View } from "react-native";

export function TodoItem({ id }: { id: string }) {
  return (
    <View>
      <Text>{id}</Text>
    </View>
  );
}
