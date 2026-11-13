export {
  useCoState,
  useAccount,
  useSuspenseCoState,
  useSuspenseAccount,
  JazzReactProvider,
} from "../classic-api.js";
export type { JazzClient } from "./create-jazz-client.js";
export {
  JazzProvider,
  type JazzProviderProps,
  JazzClientProvider,
  type JazzClientProviderProps,
  useDb,
  useJazzClient,
  useSession,
} from "./provider.js";
export { useAll, useAllSuspense, type UseAllResult, type UseAllSuspenseResult } from "./use-all.js";
export { useOne, useOneSuspense, type UseOneResult, type UseOneSuspenseResult } from "./use-one.js";
export { useAuthState, type AuthStateInfo } from "../react-core/use-auth-state.js";
export { useSyncStatus } from "../react-core/use-sync-status.js";
export type { LinkStatus, SyncStatus } from "../runtime/sync-status.js";
export type { QueryOptions, RuntimeSourcesConfig } from "../runtime/index.js";
export type { LocalFirstLoader, LocalFirstModule } from "../runtime/local-first-upgrade.js";

export * from "../accounts/exports.js";

export { createJazzClient, type JazzClientConfig } from "../session/create-jazz-session.js";
export type {
  JazzSession,
  JazzSessionSnapshot,
  JazzSessionOperation,
  JazzUser,
} from "../session/jazz-session.js";
export {
  useJazzSession,
  type UseJazzSessionResult,
  type JazzSessionViewProps,
} from "../react-core/session.js";

// Removed in alpha.61: stubs that throw, naming the replacement (one release).
export {
  AccountManager,
  JazzSessionProvider,
  createAccountManager,
  exportLocalFirstSecret,
  useAccountState,
  useJazzAuth,
  useJazzSessionOwner,
  useLocalFirstAuth,
} from "../removed-api.js";
