import { View, Text, Pressable } from "react-native";
import { ConfiguredJazzAppProvider, type JazzAppViewProps } from "../react-core/app.js";
import type { JazzAuth } from "../session/app.js";
import type { JazzClient } from "../react-native/create-jazz-client.js";
import { createJazzSession, type JazzSessionConfig } from "./create-jazz-session.js";

export type JazzProviderProps = JazzSessionConfig &
  JazzAppViewProps<JazzClient> & { auth?: JazzAuth };

/** Uses Expo SecureStore for account identity and the native runtime for data. */
export function JazzProvider({
  auth,
  children,
  signedOut,
  loading,
  error,
  ...config
}: JazzProviderProps) {
  return (
    <ConfiguredJazzAppProvider
      config={{ ...config, initial: config.initial ?? (auth ? undefined : "local-first") }}
      auth={auth}
      createJazzSession={createJazzSession}
      signedOut={signedOut}
      loading={
        loading === undefined ? (
          <View>
            <Text>Loading…</Text>
          </View>
        ) : (
          loading
        )
      }
      error={
        error === undefined
          ? (state) => (
              <View accessibilityRole="alert">
                <Text>We couldn’t connect to your account. Please try again.</Text>
                <Pressable accessibilityRole="button" onPress={() => void state.retry()}>
                  <Text>Try again</Text>
                </Pressable>
              </View>
            )
          : error
      }
    >
      {children}
    </ConfiguredJazzAppProvider>
  );
}
