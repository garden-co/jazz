import * as React from "react";
import { JazzProvider, type DbConfig } from "jazz-tools/react-native";
import { ExpoAuthSecretStore } from "jazz-tools/expo/auth-secret-store";
import {
  ActivityIndicator,
  Platform,
  Pressable,
  SafeAreaView,
  StatusBar,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { TodoList } from "./src/TodoList";
import { StressTest } from "./src/StressTest";

declare const process: {
  env: Record<string, string | undefined>;
};

const defaultServerUrl = Platform.select({
  android: "http://10.0.2.2:1625",
  ios: "http://127.0.0.1:1625",
  default: "http://127.0.0.1:1625",
});

const defaultAppId = "00000000-0000-0000-0000-000000000002";
type ExpoPublicEnvKey =
  | "EXPO_PUBLIC_JAZZ_APP_ID"
  | "EXPO_PUBLIC_JAZZ_SERVER_URL"
  | "EXPO_PUBLIC_JAZZ_ADMIN_SECRET";

const runtimeEnv = (globalThis as { process?: { env?: Record<string, string | undefined> } })
  .process?.env;

function readExpoPublicEnv(key: ExpoPublicEnvKey): string | undefined {
  const bundledValue =
    key === "EXPO_PUBLIC_JAZZ_APP_ID"
      ? typeof process !== "undefined"
        ? process.env.EXPO_PUBLIC_JAZZ_APP_ID
        : undefined
      : key === "EXPO_PUBLIC_JAZZ_SERVER_URL"
        ? typeof process !== "undefined"
          ? process.env.EXPO_PUBLIC_JAZZ_SERVER_URL
          : undefined
        : typeof process !== "undefined"
          ? process.env.EXPO_PUBLIC_JAZZ_ADMIN_SECRET
          : undefined;

  return bundledValue ?? runtimeEnv?.[key];
}

const envAppId = readExpoPublicEnv("EXPO_PUBLIC_JAZZ_APP_ID");
const envServerUrl = readExpoPublicEnv("EXPO_PUBLIC_JAZZ_SERVER_URL");
const envAdminSecret = readExpoPublicEnv("EXPO_PUBLIC_JAZZ_ADMIN_SECRET");

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
    gap: 12,
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
  tabBar: {
    flexDirection: "row",
    gap: 0,
    borderRadius: 10,
    backgroundColor: "#e5e7eb",
    padding: 3,
  },
  tab: {
    flex: 1,
    alignItems: "center",
    paddingVertical: 8,
    borderRadius: 8,
  },
  tabActive: {
    backgroundColor: "#fff",
  },
  tabText: {
    fontSize: 14,
    fontWeight: "600",
    color: "#6b7280",
  },
  tabTextActive: {
    color: "#111827",
  },
});

type Tab = "todos" | "stress";

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
  const secret = React.use(ExpoAuthSecretStore.getOrCreateSecret());
  const configKey = JSON.stringify(config ?? {});
  const resolvedConfig = React.useMemo(() => defaultConfig(secret, config), [configKey, secret]);
  const [activeTab, setActiveTab] = React.useState<Tab>("stress");

  return (
    <JazzProvider config={resolvedConfig} fallback={fallback ?? defaultFallback}>
      <SafeAreaView style={styles.container}>
        <StatusBar barStyle="dark-content" />
        <View style={styles.content}>
          <Text style={styles.title}>Jazz Stress Test</Text>

          <View style={styles.tabBar}>
            <Pressable
              style={[styles.tab, activeTab === "stress" && styles.tabActive]}
              onPress={() => setActiveTab("stress")}
            >
              <Text style={[styles.tabText, activeTab === "stress" && styles.tabTextActive]}>
                Stress Test
              </Text>
            </Pressable>
            <Pressable
              style={[styles.tab, activeTab === "todos" && styles.tabActive]}
              onPress={() => setActiveTab("todos")}
            >
              <Text style={[styles.tabText, activeTab === "todos" && styles.tabTextActive]}>
                Browse Todos
              </Text>
            </Pressable>
          </View>

          {activeTab === "stress" ? <StressTest /> : <TodoList />}
        </View>
      </SafeAreaView>
    </JazzProvider>
  );
}
