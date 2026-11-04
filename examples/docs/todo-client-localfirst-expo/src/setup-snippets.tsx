// #region context-setup-expo-minimal
import { JazzSessionProvider } from "jazz-tools/expo";
import { SafeAreaView, Text, View } from "react-native";
import { TodoList } from "./TodoList";

export function App() {
  return (
    <JazzSessionProvider
      config={{
        // Your backend's Jazz endpoint (createJazz); the app names no app id.
        serverUrl: "https://your-app.example/jazz",
        initial: "local-first",
      }}
    >
      <SafeAreaView style={{ flex: 1 }}>
        <View style={{ flex: 1, padding: 16, gap: 16 }}>
          <Text style={{ fontSize: 28, fontWeight: "700" }}>Todos</Text>
          <TodoList />
        </View>
      </SafeAreaView>
    </JazzSessionProvider>
  );
}
// #endregion context-setup-expo-minimal
