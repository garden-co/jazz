import * as React from "react";
import { JazzProvider, type DbConfig } from "jazz-tools/react-native";
import { loadOrCreateIdentitySeed, mintSelfSignedToken } from "jazz-tools";
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
const envVars = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process
  ?.env;
const envAppId = envVars?.EXPO_PUBLIC_JAZZ_APP_ID;
const envServerUrl = envVars?.EXPO_PUBLIC_JAZZ_SERVER_URL;
const envAdminSecret = envVars?.EXPO_PUBLIC_JAZZ_ADMIN_SECRET;

function defaultConfig(overrides: Partial<DbConfig> = {}): DbConfig {
  const appId = overrides.appId ?? envAppId ?? defaultAppId;
  const seed = loadOrCreateIdentitySeed(appId);
  const jwtToken = mintSelfSignedToken(seed.seed, appId);

  return {
    appId,
    env: overrides.env ?? "dev",
    userBranch: overrides.userBranch ?? "main",
    serverUrl: overrides.serverUrl ?? envServerUrl ?? defaultServerUrl,
    jwtToken,
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
  const configKey = JSON.stringify(config ?? {});
  const resolvedConfig = React.useMemo(() => defaultConfig(config), [configKey]);
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
