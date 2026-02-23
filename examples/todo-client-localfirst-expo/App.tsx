import { JazzProvider, type DbConfig } from "jazz-tools/react-native";
import {
  ActivityIndicator,
  NativeModules,
  SafeAreaView,
  StatusBar,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { TodoList } from "./src/TodoList";

function readExpoEnv(name: string): string | undefined {
  const env = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process
    ?.env;
  return env?.[name];
}

function inferMetroHost(): string | undefined {
  const sourceCode = (NativeModules as { SourceCode?: { scriptURL?: string } }).SourceCode;
  const scriptURL = sourceCode?.scriptURL;
  if (!scriptURL) return undefined;

  const match = scriptURL.match(/^https?:\/\/([^/:?#]+)(?::\d+)?/i);
  return match?.[1];
}

function resolveServerUrl(): string {
  const envUrl = readExpoEnv("EXPO_PUBLIC_JAZZ_SERVER_URL");
  if (envUrl) return envUrl;

  const metroHost = inferMetroHost();
  if (metroHost) return `http://${metroHost}:1625`;

  return "http://127.0.0.1:1625";
}

const localAuthMode =
  (readExpoEnv("EXPO_PUBLIC_JAZZ_LOCAL_MODE") as "anonymous" | "demo" | undefined) ?? "demo";

const config: DbConfig = {
  appId: readExpoEnv("EXPO_PUBLIC_JAZZ_APP_ID") ?? "00000000-0000-0000-0000-000000000002",
  serverUrl: resolveServerUrl(),
  env: "dev",
  userBranch: "main",
  localAuthMode,
  localAuthToken: readExpoEnv("EXPO_PUBLIC_JAZZ_LOCAL_TOKEN") ?? "rn-sync-user",
  adminSecret: readExpoEnv("EXPO_PUBLIC_JAZZ_ADMIN_SECRET") ?? "dev-admin-secret",
  // Optional override. If omitted, jazz-rn derives a default SurrealKV path from appId.
  // dataPath: "/absolute/path/to/todo-expo-example.surrealkv",
};

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
  return (
    <JazzProvider config={config} fallback={fallback}>
      <SafeAreaView style={styles.container}>
        <StatusBar barStyle="dark-content" />
        <View style={styles.content}>
          <Text style={styles.title}>Todos</Text>
          <TodoList
            appId={config.appId}
            localAuthMode={config.localAuthMode}
            localAuthToken={config.localAuthToken}
            jwtToken={config.jwtToken}
          />
        </View>
      </SafeAreaView>
    </JazzProvider>
  );
}
