import * as React from "react";
import { JazzProvider, type JazzClientConfig } from "jazz-tools/react-native";
import { createAccountManager } from "jazz-tools/expo";
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
const appId = envAppId ?? defaultAppId;
const serverUrl = envServerUrl ?? defaultServerUrl;
let prepared: Promise<JazzClientConfig> | undefined;
function prepareConfig() {
  return (prepared ??= createAccountManager({ appId, serverUrl }).then((accounts) => ({
    appId,
    serverUrl,
    account: accounts.getLoggedIn() ?? accounts.createLocalFirst(),
  })));
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
  config?: JazzClientConfig;
  fallback?: React.ReactNode;
};

// #region context-setup-expo
export default function App({ config, fallback }: AppProps = {}) {
  const [resolved, setResolved] = React.useState<JazzClientConfig | undefined>(config);
  const [error, setError] = React.useState<Error>();
  React.useEffect(() => {
    let active = true;
    void (config ? Promise.resolve(config) : prepareConfig()).then(
      (next) => {
        if (active) setResolved(next);
      },
      (cause) => {
        if (active) setError(cause instanceof Error ? cause : new Error(String(cause)));
      },
    );
    return () => {
      active = false;
    };
  }, [config]);
  if (error) return <Text accessibilityRole="alert">{error.message}</Text>;
  if (!resolved) return fallback ?? defaultFallback;
  return (
    <JazzProvider config={resolved} fallback={fallback ?? defaultFallback}>
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
