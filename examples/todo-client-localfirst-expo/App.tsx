import * as React from "react";
import { ExpoAuthSecretStore, withExpoDataDirectory } from "jazz-tools/expo";
import { JazzProvider, type DbConfig } from "jazz-tools/react-native";
import { ActivityIndicator, SafeAreaView, StatusBar, StyleSheet, Text, View } from "react-native";
import { TodoList } from "./src/TodoList";

// Expo's Metro bundler inlines process.env.EXPO_PUBLIC_* at bundle time.
// Set these in the shell that starts Metro.
declare const process: { env: Record<string, string | undefined> };
const defaultAppId = "00000000-0000-0000-0000-000000000002";
const envAppId = process.env.EXPO_PUBLIC_JAZZ_APP_ID;
const envServerUrl = process.env.EXPO_PUBLIC_JAZZ_SERVER_URL;
const e2eSeedTitle = process.env.EXPO_PUBLIC_JAZZ_E2E_SEED_TITLE;
const e2eSecret = process.env.EXPO_PUBLIC_JAZZ_E2E_SECRET;
const e2eAdminSecret = process.env.EXPO_PUBLIC_JAZZ_E2E_ADMIN_SECRET;

function buildConfig(secret: string): DbConfig {
  return withExpoDataDirectory({
    appId: envAppId ?? defaultAppId,
    env: "dev",
    userBranch: "main",
    driver: { type: "persistent", dbName: "todo-client-localfirst-expo" },
    secret,
    ...(envServerUrl ? { serverUrl: envServerUrl } : {}),
    ...(e2eAdminSecret ? { adminSecret: e2eAdminSecret } : {}),
  });
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
  const storedSecret = React.use(
    ExpoAuthSecretStore.getOrCreateSecret({ appId: envAppId ?? defaultAppId }),
  );
  const secret = e2eSecret ?? storedSecret;
  const config = React.useMemo(() => buildConfig(secret), [secret]);

  return (
    <JazzProvider config={config} fallback={fallback}>
      <SafeAreaView style={styles.container}>
        <StatusBar barStyle="dark-content" />
        <View style={styles.content}>
          <Text style={styles.title}>Todos</Text>
          <TodoList e2eSeedTitle={e2eSeedTitle} />
        </View>
      </SafeAreaView>
    </JazzProvider>
  );
}
