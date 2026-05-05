import * as React from "react";
import { JazzProvider, type DbConfig } from "jazz-tools/react-native";
import { ExpoAuthSecretStore } from "jazz-tools/expo";
import {
  ActivityIndicator,
  Image,
  Platform,
  SafeAreaView,
  ScrollView,
  StatusBar,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { TodoWidget } from "./src/todo-widget";
import { AuthBackup } from "./src/auth-backup";

// Expo's Metro bundler inlines process.env.EXPO_PUBLIC_* at bundle time.
// These are injected by withJazz in metro.config.mjs.
declare const process: { env: Record<string, string | undefined> };

function buildConfig(secret: string): DbConfig {
  const appId = process.env.EXPO_PUBLIC_JAZZ_APP_ID;
  const serverUrl = process.env.EXPO_PUBLIC_JAZZ_SERVER_URL;
  if (!appId || !serverUrl) {
    const missing = [
      !appId && "EXPO_PUBLIC_JAZZ_APP_ID",
      !serverUrl && "EXPO_PUBLIC_JAZZ_SERVER_URL",
    ]
      .filter((v) => !!v)
      .join(" & ");
    throw new Error(
      `${missing} not set. The withJazz Metro helper injects these at dev time; in production, set them explicitly in your environment.`,
    );
  }
  return { appId, serverUrl, secret };
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: "#f4f4f4",
    paddingTop: Platform.OS === "android" ? (StatusBar.currentHeight ?? 0) : 0,
  },
  content: {
    paddingHorizontal: 16,
    paddingTop: 20,
    paddingBottom: 40,
    gap: 16,
  },
  logo: {
    width: 80,
    height: 80 * (146 / 386),
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
  const secret = React.use(ExpoAuthSecretStore.getOrCreateSecret());
  const config = React.useMemo(() => buildConfig(secret), [secret]);

  return (
    <JazzProvider config={config} fallback={fallback}>
      <SafeAreaView style={styles.container}>
        <StatusBar barStyle="dark-content" />
        <ScrollView contentContainerStyle={styles.content}>
          <Image
            source={require("./assets/jazz-logo.png")}
            style={styles.logo}
            accessibilityLabel="Jazz"
            resizeMode="contain"
          />
          <TodoWidget />
          <AuthBackup />
        </ScrollView>
      </SafeAreaView>
    </JazzProvider>
  );
}
