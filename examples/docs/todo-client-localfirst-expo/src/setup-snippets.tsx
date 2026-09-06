// #region context-setup-expo-minimal
import { JazzProvider } from "jazz-tools/react-native";
import { createAccountManager } from "jazz-tools/expo";
import { SafeAreaView, Text, View } from "react-native";
import { TodoList } from "./TodoList";

// Await during application bootstrap, before rendering a Jazz context.
export async function prepareAccount() {
  const accounts = await createAccountManager({
    appId: "<your-app-id>",
    serverUrl: "https://your-core.example",
  });
  return accounts.getLoggedIn() ?? accounts.createLocalFirst();
}

export function App({ account }: { account: Awaited<ReturnType<typeof prepareAccount>> }) {
  return (
    <JazzProvider config={{ appId: "<your-app-id>", account }}>
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
