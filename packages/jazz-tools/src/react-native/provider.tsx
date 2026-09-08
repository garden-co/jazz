import type { ReactNode } from "react";
import { View, Text, Pressable } from "react-native";
import { ConfiguredJazzAppProvider, type JazzAppViewProps } from "../react-core/app.js";
import type { JazzAuth } from "../session/app.js";
import { createJazzSession, type JazzSessionConfig } from "./create-jazz-session.js";
import type { PublicSession } from "../runtime/context.js";
import type { Db } from "../runtime/db.js";
import {
  JazzProvider as CoreJazzProvider,
  useDb as useCoreDb,
  useJazzClient as useCoreJazzClient,
  useSession,
  type CreateJazzClient,
} from "../react-core/provider.js";
import { createJazzClient, type JazzClient as CreatedJazzClient } from "./create-jazz-client.js";
import type { JazzClientConfig as DbConfig } from "./create-jazz-client.js";

const createClient: CreateJazzClient = (config) =>
  createJazzClient(config as DbConfig) as Promise<CreatedJazzClient>;

export { JazzClientProvider, type JazzClientProviderProps } from "../react-core/provider.js";

interface JazzClientContextValue {
  db: Db;
  session: PublicSession | null;
  shutdown: CreatedJazzClient["shutdown"];
}

export type LegacyJazzProviderProps = {
  config: DbConfig;
  fallback?: ReactNode;
  children: ReactNode;
};

export type JazzAppProviderProps = JazzSessionConfig &
  JazzAppViewProps<CreatedJazzClient> & { auth?: JazzAuth };
export type JazzProviderProps = LegacyJazzProviderProps | JazzAppProviderProps;

export function JazzProvider(props: JazzProviderProps) {
  if (!("config" in props)) return <ApplicationJazzProvider {...props} />;
  const { config, fallback, children } = props;
  return (
    <CoreJazzProvider config={config} fallback={fallback} createJazzClient={createClient}>
      {children}
    </CoreJazzProvider>
  );
}

export function useJazzClient(): JazzClientContextValue {
  return useCoreJazzClient() as JazzClientContextValue;
}

export function useDb(): Db {
  return useCoreDb<Db>();
}

export { useSession };
export type { JazzClientContextValue };

function ApplicationJazzProvider({
  auth,
  children,
  signedOut,
  loading,
  error,
  ...config
}: JazzAppProviderProps) {
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
