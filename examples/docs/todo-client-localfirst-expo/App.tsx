import * as React from "react";
import { JazzProvider, type DbConfig } from "jazz-tools/react-native";
import { ExpoAuthSecretStore } from "jazz-tools/expo/auth-secret-store";
import {
  ActivityIndicator,
  Platform,
  SafeAreaView,
  StatusBar,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { TodoList } from "./src/TodoList";

const defaultServerUrl = Platform.select({
  // Android emulator cannot reach host via localhost.
  android: "http://10.0.2.2:1625",
  // iOS simulator can use host localhost directly.
  ios: "http://127.0.0.1:1625",
  default: "http://127.0.0.1:1625",
});

const defaultAppId = "00000000-0000-0000-0000-000000000002";

// Expo's Metro bundler inlines process.env.EXPO_PUBLIC_* at bundle time.
// They must be accessed as literal process.env.KEY expressions — dynamic
// lookups like globalThis.process.env[key] won't be replaced.
declare const process: { env: Record<string, string | undefined> };
const envAppId = process.env.EXPO_PUBLIC_JAZZ_APP_ID;
const envServerUrl = process.env.EXPO_PUBLIC_JAZZ_SERVER_URL;
const envAdminSecret = process.env.EXPO_PUBLIC_JAZZ_ADMIN_SECRET;

function defaultConfig(secret: string, overrides: Partial<DbConfig> = {}): DbConfig {
  const appId = overrides.appId ?? envAppId ?? defaultAppId;

  return {
    appId,
    env: overrides.env ?? "dev",
    userBranch: overrides.userBranch ?? "main",
    serverUrl: overrides.serverUrl ?? envServerUrl ?? defaultServerUrl,
    auth: { localFirstSecret: secret },
    adminSecret: overrides.adminSecret ?? envAdminSecret,
    ...overrides,
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

const defaultFallback = (
  <SafeAreaView style={styles.container}>
    <View style={styles.loadingContainer}>
      <ActivityIndicator size="small" />
      <Text style={styles.loadingText}>Loading Jazz runtime...</Text>
    </View>
  </SafeAreaView>
);

type AppProps = {
  config?: Partial<DbConfig>;
  fallback?: React.ReactNode;
};

// #region context-setup-expo
export default function App({ config, fallback }: AppProps = {}) {
  const secret = React.use(ExpoAuthSecretStore.getOrCreateSecret());
  const configKey = JSON.stringify(config ?? {});
  const resolvedConfig = React.useMemo(() => defaultConfig(secret, config), [configKey, secret]);
  return (
    <JazzProvider config={resolvedConfig} fallback={fallback ?? defaultFallback}>
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
// #endregion context-setup-expo
