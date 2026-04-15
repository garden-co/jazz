import * as React from "react";
import { JazzProvider, type DbConfig } from "jazz-tools/react-native";
import { ExpoAuthSecretStore } from "jazz-tools/expo/auth-secret-store";
import { ActivityIndicator, SafeAreaView, StatusBar, StyleSheet, Text, View } from "react-native";
import { TodoList } from "./src/TodoList";

// Expo's Metro bundler inlines process.env.EXPO_PUBLIC_* at bundle time.
// These are injected by withJazzExpo in metro.config.js.
declare const process: { env: Record<string, string | undefined> };

function buildConfig(secret: string): DbConfig {
  return {
    appId: process.env.EXPO_PUBLIC_JAZZ_APP_ID!,
    serverUrl: process.env.EXPO_PUBLIC_JAZZ_SERVER_URL!,
    env: "dev",
    userBranch: "main",
    auth: { localFirstSecret: secret },
  };
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: "#f4f4f4",
  },
  content: {
    flex: 1,
    paddingHorizontal: 16,
    paddingTop: 20,
    gap: 16,
  },
  title: {
    fontSize: 28,
    fontWeight: "700",
    color: "#111827",
  },
  loadingContainer: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    gap: 8,
  },
  loadingText: {
    color: "#374151",
    fontSize: 14,
  },
});

const fallback = (
  <SafeAreaView style={styles.container}>
    <View style={styles.loadingContainer}>
      <ActivityIndicator size="small" />
      <Text style={styles.loadingText}>Loading Jazz runtime...</Text>
    </View>
  </SafeAreaView>
);

export default function App() {
  const secret = React.use(ExpoAuthSecretStore.getOrCreateSecret());
  const config = React.useMemo(() => buildConfig(secret), [secret]);

  return (
    <JazzProvider config={config} fallback={fallback}>
      <SafeAreaView style={styles.container}>
        <StatusBar barStyle="dark-content" />
        <View style={styles.content}>
          <Text style={styles.title}>Todos</Text>
          <TodoList />
        </View>
      </SafeAreaView>
    </JazzProvider>
  );
}
