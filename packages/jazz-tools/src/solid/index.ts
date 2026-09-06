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
export { AccountAuthError, exportLocalFirstSecret, type JWTAuth } from "../accounts/enrollment.js";
export type { AccountStore } from "../accounts/persistence.js";

export { createAccountState } from "./create-account-state.js";
