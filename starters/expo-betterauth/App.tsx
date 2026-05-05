import * as React from "react";
import {
  ActivityIndicator,
  Image,
  Platform,
  Pressable,
  SafeAreaView,
  ScrollView,
  StatusBar,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { JazzProvider, useDb, type DbConfig } from "jazz-tools/react-native";
import { authClient, useSession } from "./src/auth-client";
import { SignInForm } from "./src/sign-in-form";
import { TodoWidget } from "./src/todo-widget";

declare const process: { env: Record<string, string | undefined> };

function buildConfig(jwtToken: string): DbConfig {
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
  return { appId, serverUrl, jwtToken };
}

function JwtRefresh() {
  const db = useDb();
  React.useEffect(
    () =>
      db.onAuthChanged((state) => {
        if (state.error !== "expired") return;
        authClient.token().then(({ data, error }) => {
          if (!error && data?.token) db.updateAuthToken(data.token);
        });
      }),
    [db],
  );
  return null;
}

function BetterAuthProvider({ children }: { children: React.ReactNode }) {
  const { data: session } = useSession();
  const [config, setConfig] = React.useState<DbConfig | null>(null);

  React.useEffect(() => {
    if (!session) {
      setConfig(null);
      return;
    }
    let cancelled = false;
    authClient.token().then(({ data, error }) => {
      if (cancelled || error || !data?.token) return;
      setConfig(buildConfig(data.token));
    });
    return () => {
      cancelled = true;
    };
  }, [session]);

  if (!session) return <>{children}</>;
  if (!config) {
    return (
      <View style={styles.loadingContainer}>
        <ActivityIndicator size="small" />
        <Text style={styles.loadingText}>Loading Jazz runtime...</Text>
      </View>
    );
  }

  return (
    <JazzProvider config={config} fallback={null}>
      <JwtRefresh />
      {children}
    </JazzProvider>
  );
}

export default function App() {
  const { data: session, isPending } = useSession();

  if (isPending) {
    return (
      <SafeAreaView style={styles.container}>
        <View style={styles.loadingContainer}>
          <ActivityIndicator size="small" />
          <Text style={styles.loadingText}>Loading...</Text>
        </View>
      </SafeAreaView>
    );
  }

  return (
    <BetterAuthProvider>
      <SafeAreaView style={styles.container}>
        <StatusBar barStyle="dark-content" />
        <ScrollView contentContainerStyle={styles.content}>
          <View style={styles.header}>
            <Image
              source={require("./assets/jazz-logo.png")}
              style={styles.logo}
              accessibilityLabel="Jazz"
              resizeMode="contain"
            />
            {session ? (
              <View style={styles.authRow}>
                <Text style={styles.greeting}>Hello, {session.user.name}</Text>
                <Pressable
                  onPress={async () => {
                    await authClient.signOut();
                  }}
                  style={styles.signOutButton}
                >
                  <Text style={styles.signOutText}>Sign out</Text>
                </Pressable>
              </View>
            ) : null}
          </View>
          {session ? <TodoWidget /> : <SignInForm />}
        </ScrollView>
      </SafeAreaView>
    </BetterAuthProvider>
  );
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
  header: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  logo: {
    width: 80,
    height: 80 * (146 / 386),
  },
  authRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 10,
  },
  greeting: {
    color: "#374151",
    fontSize: 13,
  },
  signOutButton: {
    paddingHorizontal: 12,
    paddingVertical: 8,
    borderRadius: 8,
    borderWidth: 1,
    borderColor: "#d1d5db",
    backgroundColor: "#fff",
  },
  signOutText: {
    color: "#111827",
    fontWeight: "500",
  },
  loadingContainer: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    gap: 8,
    paddingVertical: 40,
  },
  loadingText: {
    color: "#374151",
    fontSize: 14,
  },
});
