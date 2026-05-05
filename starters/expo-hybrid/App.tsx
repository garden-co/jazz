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
import { ExpoAuthSecretStore } from "jazz-tools/expo";
import { authClient, useSession } from "./src/auth-client";
import { SignInForm } from "./src/sign-in-form";
import { SignUpForm } from "./src/sign-up-form";
import { TodoWidget } from "./src/todo-widget";
import { AuthBackup } from "./src/auth-backup";

declare const process: { env: Record<string, string | undefined> };

type Screen = "dashboard" | "signin" | "signup";

function baseConfig(): Omit<DbConfig, "secret" | "jwtToken"> {
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
  return { appId, serverUrl };
}

async function buildLocalFirstConfig(): Promise<DbConfig> {
  const secret = await ExpoAuthSecretStore.getOrCreateSecret();
  return { ...baseConfig(), secret };
}

async function buildJwtConfig(): Promise<DbConfig | null> {
  const { data, error } = await authClient.token();
  if (error || !data?.token) return null;
  return { ...baseConfig(), jwtToken: data.token };
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

function HybridProvider({ children }: { children: React.ReactNode }) {
  const { data: authSession, isPending } = useSession();
  const [config, setConfig] = React.useState<DbConfig | null>(null);
  const authenticated = Boolean(authSession?.session);

  React.useEffect(() => {
    if (isPending) return;
    let cancelled = false;

    (async () => {
      const next = authenticated ? await buildJwtConfig() : await buildLocalFirstConfig();
      if (!cancelled && next) setConfig(next);
    })();

    return () => {
      cancelled = true;
    };
  }, [isPending, authenticated]);

  if (isPending || !config) {
    return (
      <View style={styles.loadingContainer}>
        <ActivityIndicator size="small" />
        <Text style={styles.loadingText}>Loading Jazz runtime...</Text>
      </View>
    );
  }

  return (
    <JazzProvider config={config} fallback={null}>
      {authenticated ? <JwtRefresh /> : null}
      {children}
    </JazzProvider>
  );
}

function Dashboard() {
  const { data: session } = useSession();
  const [view, setView] = React.useState<Screen>("dashboard");

  async function handleSignOut() {
    await ExpoAuthSecretStore.clearSecret();
    await authClient.signOut();
    setView("dashboard");
  }

  if (!session && view === "signup") {
    return <SignUpForm onToggle={() => setView("signin")} />;
  }
  if (!session && view === "signin") {
    return <SignInForm onToggle={() => setView("signup")} />;
  }

  return (
    <View style={styles.dashboard}>
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
            <Pressable onPress={handleSignOut} style={styles.secondaryButton}>
              <Text style={styles.secondaryButtonText}>Sign out</Text>
            </Pressable>
          </View>
        ) : (
          <View style={styles.authRow}>
            <Pressable onPress={() => setView("signup")} style={styles.linkButton}>
              <Text style={styles.linkText}>Sign up</Text>
            </Pressable>
            <Text style={styles.greeting}>or</Text>
            <Pressable onPress={() => setView("signin")} style={styles.linkButton}>
              <Text style={styles.linkText}>Sign in</Text>
            </Pressable>
          </View>
        )}
      </View>
      <TodoWidget />
      {!session ? <AuthBackup /> : null}
    </View>
  );
}

export default function App() {
  return (
    <SafeAreaView style={styles.container}>
      <StatusBar barStyle="dark-content" />
      <ScrollView contentContainerStyle={styles.content}>
        <HybridProvider>
          <Dashboard />
        </HybridProvider>
      </ScrollView>
    </SafeAreaView>
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
  dashboard: {
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
    gap: 8,
  },
  greeting: {
    color: "#374151",
    fontSize: 13,
  },
  secondaryButton: {
    paddingHorizontal: 12,
    paddingVertical: 8,
    borderRadius: 8,
    borderWidth: 1,
    borderColor: "#d1d5db",
    backgroundColor: "#fff",
  },
  secondaryButtonText: {
    color: "#111827",
    fontWeight: "500",
  },
  linkButton: {
    paddingHorizontal: 4,
    paddingVertical: 4,
  },
  linkText: {
    color: "#2563eb",
    fontWeight: "500",
  },
  loadingContainer: {
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
