import type { JazzAppViewProps } from "../react-core/app.js";
import type { JazzClient } from "../react-native/create-jazz-client.js";
import { NativeJazzAppProvider } from "../react-native/provider.js";
import { createJazzClient, type JazzClientConfig } from "./create-jazz-session.js";

export type JazzProviderProps = JazzClientConfig & JazzAppViewProps<JazzClient>;

/** Uses Expo SecureStore for account identity and the native runtime for data. */
export function JazzProvider(props: JazzProviderProps) {
  const { children, signedOut, loading, error, ...config } = props;
  const view = { children, signedOut, loading, error };
  return <NativeJazzAppProvider {...view} config={config} createJazzClient={createJazzClient} />;
}
