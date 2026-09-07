import {
  JazzSessionProvider as ExternalJazzSessionProvider,
  ConfiguredJazzSessionProvider,
  type JazzSessionProviderProps as ExternalProps,
  type ConfiguredJazzSessionProviderProps,
} from "../react-core/session.js";
import { createJazzSession, type JazzSessionConfig } from "./create-jazz-session.js";
import type { JazzClient } from "../react-native/create-jazz-client.js";
export { createJazzSession, type JazzSessionConfig } from "./create-jazz-session.js";
import {
  useJazzSession as useCoreJazzSession,
  type UseJazzSessionResult as CoreResult,
} from "../react-core/session.js";
export type UseJazzSessionResult = CoreResult<JazzClient>;
export function useJazzSession(): UseJazzSessionResult {
  return useCoreJazzSession<JazzClient>();
}
export type { JazzSession, JazzSessionSnapshot, JazzSessionActions } from "../session/state.js";

export type JazzSessionProviderProps =
  | ExternalProps<JazzClient>
  | ConfiguredJazzSessionProviderProps<JazzSessionConfig>;

export function JazzSessionProvider(props: JazzSessionProviderProps) {
  return "session" in props ? (
    <ExternalJazzSessionProvider {...props} />
  ) : (
    <ConfiguredJazzSessionProvider {...props} createJazzSession={createJazzSession} />
  );
}
