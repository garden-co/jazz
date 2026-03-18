import { JazzProvider } from "jazz-tools/react-native";
import { SafeAreaView, Text, View } from "react-native";
import { TodoList } from "./TodoList";

// #region context-setup-expo-minimal
export function AppMinimal() {
  return (
    <JazzProvider
      config={{
        appId: "00000000-0000-0000-0000-000000000002",
        serverUrl: "http://10.0.2.2:1625",
        localAuthMode: "demo",
        // jwtToken: authToken, // Use this (instead of localAuthMode) for external auth.
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
