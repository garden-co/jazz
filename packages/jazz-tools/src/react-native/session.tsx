import {
  createJazzSessionBindings,
  type JazzSessionProviderProps as ExternalProps,
  type ConfiguredJazzSessionProviderProps,
  type UseJazzSessionResult as CoreResult,
} from "../react-core/session.js";
import { createJazzSession, type JazzSessionConfig } from "./create-jazz-session.js";
import type { JazzClient } from "./create-jazz-client.js";
export { createJazzSession, type JazzSessionConfig } from "./create-jazz-session.js";
export type { JazzSession, JazzSessionSnapshot, JazzSessionActions } from "../session/state.js";
export type { JazzSessionOwnerResult } from "../react-core/session.js";

export type UseJazzSessionResult = CoreResult<JazzClient>;
export type JazzSessionProviderProps =
  | ExternalProps<JazzClient>
  | ConfiguredJazzSessionProviderProps<JazzSessionConfig>;

export const { JazzSessionProvider, useJazzSession, useJazzSessionOwner } =
  createJazzSessionBindings(createJazzSession);
