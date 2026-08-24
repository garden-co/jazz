import * as React from "react";
import { type DbConfig } from "jazz-tools";
import { JazzProvider } from "jazz-tools/react";
import {
  ActivityIndicator,
  Pressable,
  SafeAreaView,
  StatusBar,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { ExpoAuthSecretStore } from "./src/expo-auth-secret-store";
import { createResettablePromise, loadSecret, SecretLoadError } from "./src/secret-promise-cache";
import { TodoList } from "./src/TodoList";

// Expo's Metro bundler inlines process.env.EXPO_PUBLIC_* at bundle time.
// Set these in the shell that starts Metro.
declare const process: { env: Record<string, string | undefined> };

function buildConfig(secret: string): DbConfig {
  return {
    appId: process.env.EXPO_PUBLIC_JAZZ_APP_ID!,
    serverUrl: process.env.EXPO_PUBLIC_JAZZ_SERVER_URL!,
    env: "dev",
    secret,
  };
}

const authSecret = createResettablePromise(() =>
  loadSecret(() => ExpoAuthSecretStore.getOrCreateSecret()),
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
  const secret = React.use(authSecret.get());
  const config = React.useMemo(() => buildConfig(secret), [secret]);

  return (
    <JazzProvider config={config} fallback={runtimeFallback}>
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

export default function AppRoot() {
  return (
    <SecretLoadErrorBoundary>
      <React.Suspense fallback={authFallback}>
        <App />
      </React.Suspense>
    </SecretLoadErrorBoundary>
  );
}
