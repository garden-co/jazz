import { View, Text, Pressable } from "react-native";
import type { PublicSession } from "../runtime/context.js";
import type { Db } from "../runtime/db.js";
import {
  useDb as useCoreDb,
  useJazzClient as useCoreJazzClient,
  useSession,
} from "../react-core/provider.js";
import {
  JazzSessionProvider,
  type JazzSessionSourceProps,
  type JazzSessionViewProps,
} from "../react-core/session.js";
import type { SessionCore } from "../session/state.js";
import { refuseRemovedProviderProps } from "../react/provider-props.js";
import { openNativeSessionCore, type JazzClientConfig } from "./create-jazz-session.js";
import type { JazzClient as CreatedJazzClient } from "./create-jazz-client.js";

export { JazzClientProvider, type JazzClientProviderProps } from "../react-core/provider.js";

interface JazzClientContextValue {
  db: Db;
  session: PublicSession | null;
  shutdown: CreatedJazzClient["shutdown"];
}

export type JazzProviderProps = JazzSessionSourceProps<JazzClientConfig, CreatedJazzClient> &
  JazzSessionViewProps<CreatedJazzClient>;

/**
 * Jazz for a React Native app: owns a session (`config`) or shows yours
 * (`session`), renders `children` with its client once someone is signed in,
 * and `signedOut`, `loading` or `error` otherwise. Read the session anywhere
 * below with `useJazzSession()`.
 */
export function JazzProvider(props: JazzProviderProps) {
  return <NativeJazzProvider {...props} openCore={openNativeSessionCore} />;
}

export function useJazzClient(): JazzClientContextValue {
  return useCoreJazzClient() as JazzClientContextValue;
}

export function useDb(): Db {
  return useCoreDb<Db>();
}

export { useSession };
export type { JazzClientContextValue };

/** @internal The provider shared by React Native and Expo, with native default views. */
export function NativeJazzProvider<Config>(
  props: JazzSessionSourceProps<Config, CreatedJazzClient> &
    JazzSessionViewProps<CreatedJazzClient> & {
      openCore: (config: Config) => Promise<SessionCore<CreatedJazzClient>>;
    },
) {
  refuseRemovedProviderProps(props);
  const { loading, error, ...rest } = props;
  return (
    <JazzSessionProvider
      {...rest}
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
                <Text>Jazz couldn’t start. Please try again.</Text>
                <Pressable
                  accessibilityRole="button"
                  onPress={() => void state.retry().catch(() => {})}
                >
                  <Text>Try again</Text>
                </Pressable>
              </View>
            )
          : error
      }
    />
  );
}
