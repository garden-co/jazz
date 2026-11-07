import type { ReactNode } from "react";
import { View, Text, Pressable } from "react-native";
import { ConfiguredJazzAppProvider, type JazzAppViewProps } from "../react-core/app.js";
import type { JazzSession } from "../session/state.js";
import { createJazzClient, type JazzClientConfig } from "./create-jazz-session.js";
import type { PublicSession } from "../runtime/context.js";
import type { Db } from "../runtime/db.js";
import {
  JazzProvider as CoreJazzProvider,
  useDb as useCoreDb,
  useJazzClient as useCoreJazzClient,
  useSession,
  type OpenJazzClient,
} from "../react-core/provider.js";
import { openJazzClient, type JazzClient as CreatedJazzClient } from "./create-jazz-client.js";
import type { OpenJazzClientConfig as DbConfig } from "./create-jazz-client.js";

const createClient: OpenJazzClient = (config) =>
  openJazzClient(config as DbConfig) as Promise<CreatedJazzClient>;

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

export type JazzAppProviderProps = JazzClientConfig & JazzAppViewProps<CreatedJazzClient>;
export type JazzProviderProps = LegacyJazzProviderProps | JazzAppProviderProps;

export function JazzProvider(props: JazzProviderProps) {
  if (!("config" in props)) return <ApplicationJazzProvider {...props} />;
  const { config, fallback, children } = props;
  return (
    <CoreJazzProvider config={config} fallback={fallback} openJazzClient={createClient}>
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

function ApplicationJazzProvider(props: JazzAppProviderProps) {
  const { children, signedOut, loading, error, ...config } = props;
  const view = { children, signedOut, loading, error };
  return <NativeJazzAppProvider {...view} config={config} createJazzClient={createJazzClient} />;
}

/** The app shell shared by React Native and Expo, with native default loading and error views. */
export function NativeJazzAppProvider<Config extends { initial?: unknown }>({
  config,
  createJazzClient,
  loading,
  error,
  ...view
}: JazzAppViewProps<CreatedJazzClient> & {
  config: Config;
  createJazzClient: (config: Config) => Promise<JazzSession<CreatedJazzClient>>;
}) {
  return (
    <ConfiguredJazzAppProvider
      {...view}
      config={config}
      createJazzClient={createJazzClient}
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
    />
  );
}
