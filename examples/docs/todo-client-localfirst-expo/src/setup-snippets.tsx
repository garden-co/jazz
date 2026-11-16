// #region context-setup-expo-minimal
import { JazzProvider } from "jazz-tools/expo";
import { SafeAreaView, Text, View } from "react-native";
import { TodoList } from "./TodoList";

export function App() {
  return (
    <JazzProvider
      config={{
        // Your backend's Jazz endpoint (createJazz); the app names no app id.
        serverUrl: "https://your-app.example/jazz",
        // Everyone starts as a guest: a device account, kept in SecureStore.
        initial: "device",
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
