import * as React from "react";
import { JazzSessionProvider, useJazzSession } from "jazz-tools/expo";
import {
  ActivityIndicator,
  Pressable,
  SafeAreaView,
  StatusBar,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { TodoList } from "./src/TodoList";

// Expo's Metro bundler inlines process.env.EXPO_PUBLIC_* at bundle time.
// Set these in the shell that starts Metro.
declare const process: { env: Record<string, string | undefined> };

const appId = process.env.EXPO_PUBLIC_JAZZ_APP_ID!;
const serverUrl = process.env.EXPO_PUBLIC_JAZZ_SERVER_URL!;
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
  errorText: {
    color: "#991b1b",
    fontSize: 14,
    textAlign: "center",
  },
  retryButton: {
    backgroundColor: "#111827",
    borderRadius: 6,
    paddingHorizontal: 16,
    paddingVertical: 10,
  },
  retryButtonText: {
    color: "#ffffff",
    fontSize: 14,
    fontWeight: "600",
  },
});

function SessionFallback() {
  const { error, retry } = useJazzSession();
  return (
    <SafeAreaView style={styles.container}>
      <View style={styles.loadingContainer}>
        {error ? (
          <>
            <Text style={styles.errorText}>{error.message}</Text>
            <Pressable
              accessibilityRole="button"
              onPress={() => void retry().catch(() => {})}
              style={styles.retryButton}
            >
              <Text style={styles.retryButtonText}>Try again</Text>
            </Pressable>
          </>
        ) : (
          <>
            <ActivityIndicator size="small" />
            <Text style={styles.loadingText}>Loading secure credentials and Jazz runtime...</Text>
          </>
        )}
      </View>
    </SafeAreaView>
  );
}

export function App() {
  if (!appId || !serverUrl)
    throw new Error("Set EXPO_PUBLIC_JAZZ_APP_ID and EXPO_PUBLIC_JAZZ_SERVER_URL");
  return (
    <JazzSessionProvider
      config={{ appId, serverUrl, env: "dev", initial: "local-first" }}
      fallback={<SessionFallback />}
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

export default App;
