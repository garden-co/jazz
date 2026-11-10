import * as React from "react";
import { JazzProvider, type DbConfig } from "jazz-tools/react-native";
import { createAccountManager } from "jazz-tools/expo";
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

// The app's backend (backend.ts) serves Jazz under /jazz.
const defaultServerUrl = Platform.select({
  android: "http://10.0.2.2:1625/jazz",
  ios: "http://127.0.0.1:1625/jazz",
  default: "http://127.0.0.1:1625/jazz",
});

// Expo's Metro bundler inlines process.env.EXPO_PUBLIC_* at bundle time.
// They must be accessed as literal process.env.KEY expressions — dynamic
// lookups like globalThis.process.env[key] won't be replaced.
declare const process: { env: Record<string, string | undefined> };
const envServerUrl = process.env.EXPO_PUBLIC_JAZZ_SERVER_URL;

const accounts = new Map<string, ReturnType<typeof localFirstAccount>>();

/** This device's local-first guest of the app's backend. */
function localFirstAccount(serverUrl: string) {
  return createAccountManager({ serverUrl }).then(
    (manager) => manager.getLoggedIn() ?? manager.createLocalFirst(),
  );
}

function accountFor(serverUrl: string) {
  let account = accounts.get(serverUrl);
  if (!account) {
    account = localFirstAccount(serverUrl);
    accounts.set(serverUrl, account);
  }
  return account;
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
  const serverUrl = config?.serverUrl ?? envServerUrl ?? defaultServerUrl;
  const account = React.use(accountFor(serverUrl));
  const configKey = JSON.stringify(config ?? {});
  const resolvedConfig = React.useMemo(
    () => ({ env: "dev", ...config, serverUrl, account }),
    [configKey, serverUrl, account],
  );
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
