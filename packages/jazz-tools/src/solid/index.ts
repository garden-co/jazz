export { createJazzClient, type JazzClient } from "../web/create-jazz-client.js";
export {
  createSolidJazzClient,
  type PendingSolidJazzClient,
  type SolidJazzClient,
} from "./create-solid-jazz-client.js";
export {
  JazzClientProvider,
  JazzProvider,
  useDb,
  useAuthState,
  useJazzClient,
  useSession,
  type JazzClientProviderProps,
  type JazzProviderProps,
} from "./provider.js";

export { useAll } from "./use-all.js";
export { useOne, type UseOneResult } from "./use-one.js";
export { useLocalFirstAuth, type UseLocalFirstAuth } from "./use-local-first-auth.js";
export type { DurabilityTier, QueryOptions, RuntimeSourcesConfig } from "../runtime/index.js";

export {
  createAccountManager,
  type AccountManagerConfig,
} from "../accounts/create-account-manager.js";
export type {
  AccountHandle,
  AccountIdentity,
  AccountSnapshot,
  AccountManager,
} from "../accounts/state.js";
export {
  AccountAuthError,
  exportLocalFirstSecret,
  type BackendAuth,
  type JWTAuth,
} from "../accounts/enrollment.js";
export type { AccountStore } from "../accounts/persistence.js";

export { createAccountState } from "./create-account-state.js";

export { createJazzSession, type JazzSessionConfig } from "../session/create-jazz-session.js";
export type {
  JazzSession,
  JazzSessionActions,
  JazzSessionSnapshot,
  JazzSessionOperation,
} from "../session/state.js";
export {
  JazzSessionProvider,
  useJazzSession,
  createJazzSessionState,
  type JazzSessionProviderProps,
  type UseJazzSession,
} from "./session.js";

export { useJazzAuth, type UseJazzAuth, type JazzAppProviderProps } from "./app.js";
export { betterAuth, jwtAuth, type JazzAuth, type JazzAppSnapshot } from "../session/app.js";
export type { JazzAppConfig } from "../session/create-jazz-app.js";
