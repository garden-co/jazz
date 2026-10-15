import type { JazzAppViewProps } from "../react-core/app.js";
import type { JazzAuth } from "../session/app.js";
import type { JazzClient } from "../react-native/create-jazz-client.js";
import { NativeJazzAppProvider } from "../react-native/provider.js";
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
  const view = { auth, children, signedOut, loading, error };
  return <NativeJazzAppProvider {...view} config={config} createJazzSession={createJazzSession} />;
}
