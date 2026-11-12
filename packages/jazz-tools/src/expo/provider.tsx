import type { JazzSessionSourceProps, JazzSessionViewProps } from "../react-core/session.js";
import type { JazzClient } from "../react-native/create-jazz-client.js";
import { NativeJazzProvider } from "../react-native/provider.js";
import { openNativeSessionCore, type JazzClientConfig } from "./create-jazz-session.js";

export type JazzProviderProps = JazzSessionSourceProps<JazzClientConfig, JazzClient> &
  JazzSessionViewProps<JazzClient>;

/**
 * Jazz for an Expo app: owns a session (`config`) or shows yours (`session`).
 * Expo SecureStore keeps the device's accounts; the native runtime its data.
 */
export function JazzProvider(props: JazzProviderProps) {
  return <NativeJazzProvider {...props} openCore={openNativeSessionCore} />;
}
