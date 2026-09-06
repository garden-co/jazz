import * as React from "react";
import { createJazzClient, JazzClientProvider, type JazzClient } from "jazz-tools/react-native";
import {
  ActivityIndicator,
  Pressable,
  SafeAreaView,
  StatusBar,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { createAccountManager } from "jazz-tools/expo";
import { createResettablePromise, loadSecret, SecretLoadError } from "./src/secret-promise-cache";
import { TodoList } from "./src/TodoList";

// Expo's Metro bundler inlines process.env.EXPO_PUBLIC_* at bundle time.
// Set these in the shell that starts Metro.
declare const process: { env: Record<string, string | undefined> };

const appId = process.env.EXPO_PUBLIC_JAZZ_APP_ID!;
const serverUrl = process.env.EXPO_PUBLIC_JAZZ_SERVER_URL!;
const authSecret = createResettablePromise(() =>
  loadSecret(async () => {
    if (!appId || !serverUrl)
      throw new Error("Set EXPO_PUBLIC_JAZZ_APP_ID and EXPO_PUBLIC_JAZZ_SERVER_URL");
    const accounts = await createAccountManager({
      appId,
      serverUrl,
    });
    return accounts.getLoggedIn() ?? accounts.createLocalFirst();
  }),
);

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

const authFallback = (
  <SafeAreaView style={styles.container}>
    <View style={styles.loadingContainer}>
      <ActivityIndicator size="small" />
      <Text style={styles.loadingText}>Loading secure credentials...</Text>
    </View>
  </SafeAreaView>
);

const runtimeFallback = (
  <SafeAreaView style={styles.container}>
    <View style={styles.loadingContainer}>
      <ActivityIndicator size="small" />
      <Text style={styles.loadingText}>Loading Jazz runtime...</Text>
    </View>
  </SafeAreaView>
);

class SecretLoadErrorBoundary extends React.Component<
  React.PropsWithChildren,
  { failed: boolean }
> {
  state = { failed: false };

  static getDerivedStateFromError(error: unknown) {
    if (!(error instanceof SecretLoadError)) {
      throw error;
    }
    return { failed: true };
  }

  private retry = () => {
    authSecret.reset();
    this.setState({ failed: false });
  };

  render() {
    if (this.state.failed) {
      return (
        <SafeAreaView style={styles.container}>
          <View style={styles.loadingContainer}>
            <Text style={styles.errorText}>Could not load secure credentials.</Text>
            <Pressable accessibilityRole="button" onPress={this.retry} style={styles.retryButton}>
              <Text style={styles.retryButtonText}>Try again</Text>
            </Pressable>
          </View>
        </SafeAreaView>
      );
    }

    return this.props.children;
  }
}

export function App() {
  const account = React.use(authSecret.get());
  const [client, setClient] = React.useState<JazzClient>();
  const [error, setError] = React.useState<Error>();
  React.useEffect(() => {
    let cancelled = false;
    let active: JazzClient | undefined;
    void createJazzClient({ appId, serverUrl, env: "dev", account })
      .then(async (opened) => {
        if (cancelled) {
          await opened.shutdown();
          return;
        }
        active = opened;
        setClient(opened);
      })
      .catch((cause) => {
        if (!cancelled) setError(cause instanceof Error ? cause : new Error(String(cause)));
      });
    return () => {
      cancelled = true;
      setClient(undefined);
      void active?.shutdown().catch(console.error);
    };
  }, [account]);
  if (error) throw error;
  if (!client) return runtimeFallback;

  return (
    <JazzClientProvider client={client}>
      <SafeAreaView style={styles.container}>
        <StatusBar barStyle="dark-content" />
        <View style={styles.content}>
          <Text style={styles.title}>Todos</Text>
          <TodoList />
        </View>
      </SafeAreaView>
    </JazzClientProvider>
  );
}

export default function AppRoot() {
  return (
    <SecretLoadErrorBoundary>
      <React.Suspense fallback={authFallback}>
        <App />
      </React.Suspense>
    </SecretLoadErrorBoundary>
  );
}
