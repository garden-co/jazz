import * as React from "react";
import { getActiveSyntheticAuth, JazzProvider, type DbConfig } from "jazz-tools/react-native";
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

declare const process: {
  env: Record<string, string | undefined>;
};

type LocalAuthMode = Extract<DbConfig["localAuthMode"], "anonymous" | "demo">;

const defaultServerUrl = Platform.select({
  // Android emulator cannot reach host via localhost.
  android: "http://10.0.2.2:1625",
  // iOS simulator can use host localhost directly.
  ios: "http://127.0.0.1:1625",
  default: "http://127.0.0.1:1625",
});

const defaultAppId = "019d4349-2434-7753-b91a-21642b0896c7";
type ExpoPublicEnvKey =
  | "EXPO_PUBLIC_JAZZ_APP_ID"
  | "EXPO_PUBLIC_JAZZ_SERVER_URL"
  | "EXPO_PUBLIC_JAZZ_LOCAL_MODE"
  | "EXPO_PUBLIC_JAZZ_LOCAL_TOKEN";

const runtimeEnv = (globalThis as { process?: { env?: Record<string, string | undefined> } })
  .process?.env;

function readExpoPublicEnv(key: ExpoPublicEnvKey): string | undefined {
  // Keep direct process.env access so Expo can statically inline values at bundle time.
  const bundledValue =
    key === "EXPO_PUBLIC_JAZZ_APP_ID"
      ? typeof process !== "undefined"
        ? process.env.EXPO_PUBLIC_JAZZ_APP_ID
        : undefined
      : key === "EXPO_PUBLIC_JAZZ_SERVER_URL"
        ? typeof process !== "undefined"
          ? process.env.EXPO_PUBLIC_JAZZ_SERVER_URL
          : undefined
        : key === "EXPO_PUBLIC_JAZZ_LOCAL_MODE"
          ? typeof process !== "undefined"
            ? process.env.EXPO_PUBLIC_JAZZ_LOCAL_MODE
            : undefined
          : typeof process !== "undefined"
            ? process.env.EXPO_PUBLIC_JAZZ_LOCAL_TOKEN
            : undefined;

  return bundledValue ?? runtimeEnv?.[key];
}

const envAppId = readExpoPublicEnv("EXPO_PUBLIC_JAZZ_APP_ID");
const envServerUrl = readExpoPublicEnv("EXPO_PUBLIC_JAZZ_SERVER_URL");
const envLocalMode = readExpoPublicEnv("EXPO_PUBLIC_JAZZ_LOCAL_MODE");
const envLocalToken = readExpoPublicEnv("EXPO_PUBLIC_JAZZ_LOCAL_TOKEN");

const syntheticAuthCache = new Map<string, ReturnType<typeof getActiveSyntheticAuth>>();

function parseLocalAuthMode(mode: string | undefined): LocalAuthMode | undefined {
  if (mode === "anonymous" || mode === "demo") return mode;
  return undefined;
}

function getStableSyntheticAuth(appId: string) {
  const cached = syntheticAuthCache.get(appId);
  if (cached) return cached;
  const created = getActiveSyntheticAuth(appId, { defaultMode: "demo" });
  syntheticAuthCache.set(appId, created);
  return created;
}

function defaultConfig(overrides: Partial<DbConfig> = {}): DbConfig {
  const appId = overrides.appId ?? envAppId ?? defaultAppId;
  const syntheticAuth = getStableSyntheticAuth(appId);
  const envMode = parseLocalAuthMode(envLocalMode);

  return {
    appId,
    env: overrides.env ?? "dev",
    userBranch: overrides.userBranch ?? "main",
    serverUrl: overrides.serverUrl ?? envServerUrl ?? defaultServerUrl,
    localAuthMode: overrides.localAuthMode ?? envMode ?? syntheticAuth.localAuthMode,
    localAuthToken: overrides.localAuthToken ?? envLocalToken ?? syntheticAuth.localAuthToken,
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
