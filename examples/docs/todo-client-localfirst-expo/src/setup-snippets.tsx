// #region context-setup-expo-minimal
import { JazzProvider } from "jazz-tools/react-native";
import { SafeAreaView, Text, View } from "react-native";
import { TodoList } from "./TodoList";

export function App() {
  return (
    <JazzProvider
      config={{
        appId: "my-todo-app",
      }}
    >
      <SafeAreaView style={{ flex: 1 }}>
        <View style={{ flex: 1, padding: 16, gap: 16 }}>
          <Text style={{ fontSize: 28, fontWeight: "700" }}>Todos</Text>
          <TodoList />
        </View>
      </SafeAreaView>
    </JazzProvider>
  );
}
// #endregion context-setup-expo-minimal
