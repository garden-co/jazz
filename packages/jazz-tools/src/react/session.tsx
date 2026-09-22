import {
  JazzSessionProvider as ExternalJazzSessionProvider,
  ConfiguredJazzSessionProvider,
  type JazzSessionProviderProps as ExternalProps,
  type ConfiguredJazzSessionProviderProps,
} from "../react-core/session.js";
import { jazzDevPluginActive } from "../dev-tools/auto-attach.js";
import { createJazzSession, type JazzSessionConfig } from "../session/create-jazz-session.js";
import type { JazzClient } from "./create-jazz-client.js";
import { DevToolsAutoAttach } from "./devtools-auto-attach.js";
export { createJazzSession, type JazzSessionConfig } from "../session/create-jazz-session.js";
import {
  useJazzSession as useCoreJazzSession,
  type UseJazzSessionResult as CoreResult,
} from "../react-core/session.js";
export type UseJazzSessionResult = CoreResult<JazzClient>;
export function useJazzSession(): UseJazzSessionResult {
  return useCoreJazzSession<JazzClient>();
}
export type { JazzSession, JazzSessionSnapshot, JazzSessionActions } from "../session/state.js";

type JazzSessionProviderOptions = {
  /** Dev-only: auto-open the inspector overlay. Default true. */
  autoAttachDevTools?: boolean;
};

export type JazzSessionProviderProps =
  | (ExternalProps<JazzClient> & JazzSessionProviderOptions)
  | (ConfiguredJazzSessionProviderProps<JazzSessionConfig> & JazzSessionProviderOptions);

export function JazzSessionProvider(props: JazzSessionProviderProps) {
  const { autoAttachDevTools, children, ...sessionProps } = props;
  const shouldAutoAttach = process.env.NODE_ENV !== "production" && autoAttachDevTools !== false;
  const devTools = shouldAutoAttach ? <DevToolsAutoAttach /> : null;

  if ("session" in sessionProps) {
    return (
      <ExternalJazzSessionProvider {...sessionProps}>
        {devTools}
        {children}
      </ExternalJazzSessionProvider>
    );
  }

  const effectiveConfig =
    shouldAutoAttach && sessionProps.config.devMode === undefined && jazzDevPluginActive()
      ? { ...sessionProps.config, devMode: true }
      : sessionProps.config;

  return (
    <ConfiguredJazzSessionProvider
      {...sessionProps}
      config={effectiveConfig}
      createJazzSession={createJazzSession}
    >
      {devTools}
      {children}
    </ConfiguredJazzSessionProvider>
  );
}

import { useJazzSessionOwner as useCoreJazzSessionOwner } from "../react-core/session.js";
export type { JazzSessionOwnerResult } from "../react-core/session.js";
/** Capture configuration once and retain the owner across authentication UI transitions. */
export function useJazzSessionOwner(config: JazzSessionConfig) {
  return useCoreJazzSessionOwner(config, createJazzSession);
}
