import * as React from "react";
import {
  createJazzClient,
  getActiveSyntheticAuth,
  JazzProvider,
  type StorageLike,
} from "jazz-tools/react-native";
import {
  ActivityIndicator,
  NativeModules,
  Platform,
  SafeAreaView,
  StatusBar,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { TodoList } from "./src/TodoList";

type JazzProviderClientConfig = NonNullable<Parameters<typeof createJazzClient>[0]>;
type LocalAuthMode = Extract<JazzProviderClientConfig["localAuthMode"], "anonymous" | "demo">;

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
const envLocalMode = envVars?.EXPO_PUBLIC_JAZZ_LOCAL_MODE;
const envLocalToken = envVars?.EXPO_PUBLIC_JAZZ_LOCAL_TOKEN;

function defaultConfig(
  overrides: Partial<JazzProviderClientConfig> = {},
): JazzProviderClientConfig {
  const appId = overrides.appId ?? envAppId ?? defaultAppId;
  const modeFromEnv = envLocalMode ?? "demo";
  const tokenFromEnv = envLocalToken ?? 'device-token-123';

  return {
    appId,
    env: "dev",
    userBranch: "main",
    serverUrl: envServerUrl ?? defaultServerUrl,
    localAuthMode: modeFromEnv as LocalAuthMode,
    localAuthToken: tokenFromEnv,
    adminSecret: envAdminSecret,
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
  config?: Partial<JazzProviderClientConfig>;
  fallback?: React.ReactNode;
};

export default function App({ config, fallback }: AppProps = {}) {
  const resolvedConfig = defaultConfig(config);
  const configKey = JSON.stringify(resolvedConfig);
  const [client, setClient] = React.useState<Awaited<ReturnType<typeof createJazzClient>> | null>(
    null,
  );
  const [error, setError] = React.useState<unknown>(null);

  React.useEffect(() => {
    let active = true;
    const pending = createJazzClient(resolvedConfig);

    void pending.then(
      (resolved) => {
        if (!active) {
          void resolved.shutdown();
          return;
        }
        setClient(resolved);
      },
      (reason) => {
        if (!active) return;
        setError(reason);
      },
    );

    return () => {
      active = false;
      void pending.then((resolved) => resolved.shutdown()).catch(() => {});
    };
  }, [configKey]);

  if (error) {
    throw error;
  }

  if (!client) {
    return <>{fallback ?? defaultFallback}</>;
  }

  return (
    <JazzProvider client={client}>
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
