import {
  JazzSessionProvider as ExternalJazzSessionProvider,
  ConfiguredJazzSessionProvider,
  type JazzSessionProviderProps as ExternalProps,
  type ConfiguredJazzSessionProviderProps,
} from "../react-core/session.js";
import { createJazzClient, type JazzClientConfig } from "../session/create-jazz-session.js";
import type { JazzClient } from "./create-jazz-client.js";
import { refuseAutoAttachDevTools } from "../runtime/removed-provider-props.js";
export { createJazzClient, type JazzClientConfig } from "../session/create-jazz-session.js";
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
  | ConfiguredJazzSessionProviderProps<JazzClientConfig>;

export function JazzSessionProvider(props: JazzSessionProviderProps) {
  refuseAutoAttachDevTools("JazzSessionProvider", props);
  if ("session" in props) return <ExternalJazzSessionProvider {...props} />;
  return <ConfiguredJazzSessionProvider {...props} createJazzClient={createJazzClient} />;
}

import { useJazzSessionOwner as useCoreJazzSessionOwner } from "../react-core/session.js";
export type { JazzSessionOwnerResult } from "../react-core/session.js";
/** Capture configuration once and retain the owner across authentication UI transitions. */
export function useJazzSessionOwner(config: JazzClientConfig) {
  return useCoreJazzSessionOwner(config, createJazzClient);
}
