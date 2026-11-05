import * as React from "react";
import { JazzSessionProvider, type JazzClientConfig } from "jazz-tools/expo";
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

// The app's backend (`pnpm backend`), as the simulator reaches it.
const defaultServerUrl = Platform.select({
  // Android emulator cannot reach host via localhost.
  android: "http://10.0.2.2:1625/jazz",
  // iOS simulator can use host localhost directly.
  ios: "http://127.0.0.1:1625/jazz",
  default: "http://127.0.0.1:1625/jazz",
});

// Expo's Metro bundler inlines process.env.EXPO_PUBLIC_* at bundle time.
// They must be accessed as literal process.env.KEY expressions — dynamic
// lookups like globalThis.process.env[key] won't be replaced.
declare const process: { env: Record<string, string | undefined> };
const envServerUrl = process.env.EXPO_PUBLIC_JAZZ_SERVER_URL;
const serverUrl = envServerUrl ?? defaultServerUrl;
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
  return (
    <JazzSessionProvider
      config={config ?? { serverUrl, initial: "local-first" }}
      fallback={fallback ?? defaultFallback}
    >
      <SafeAreaView style={styles.container}>
        <StatusBar barStyle="dark-content" />
        <View style={styles.content}>
          <Text style={styles.title}>Todos</Text>
          <TodoList />
        </View>
      </SafeAreaView>
    </JazzSessionProvider>
  );
}
// #endregion context-setup-expo
