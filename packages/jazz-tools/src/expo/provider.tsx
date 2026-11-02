import type { JazzAppViewProps } from "../react-core/app.js";
import { authForUser, type JazzAuth } from "../session/app.js";
import type { JazzClient } from "../react-native/create-jazz-client.js";
import { NativeJazzAppProvider } from "../react-native/provider.js";
import { createJazzSession, type JazzSessionConfig } from "./create-jazz-session.js";

export type JazzProviderProps = JazzSessionConfig &
  JazzAppViewProps<JazzClient> & {
    auth?: JazzAuth;
    /** Who your auth says is signed in: `undefined` loading, `null` signed out, else the user's id. */
    user?: string | null;
  };

/** Uses Expo SecureStore for account identity and the native runtime for data. */
export function JazzProvider(props: JazzProviderProps) {
  const { auth: explicitAuth, user, children, signedOut, loading, error, ...config } = props;
  const auth = authForUser(Object.hasOwn(props, "user"), user, explicitAuth);
  const view = { auth, children, signedOut, loading, error };
  return <NativeJazzAppProvider {...view} config={config} createJazzSession={createJazzSession} />;
}
