export { useCoState, useAccount, useSuspenseCoState, useSuspenseAccount } from "../classic-api.js";
export type { JazzClient } from "../react/create-jazz-client.js";
export {
  JazzClientProvider,
  useDb,
  useJazzClient,
  useSession,
  type JazzClientProviderProps,
} from "./provider.js";
export { useAll, useAllSuspense, type UseAllResult, type UseAllSuspenseResult } from "./use-all.js";
export { useOne, useOneSuspense, type UseOneResult, type UseOneSuspenseResult } from "./use-one.js";
export { useAuthState, type AuthStateInfo } from "./use-auth-state.js";
export { useSyncStatus } from "./use-sync-status.js";

export type { DurabilityTier, QueryBuilder, QueryOptions, TableProxy } from "../runtime/index.js";

export {
  // The provider a host binding builds its `<JazzProvider>` on (it takes the
  // host's `openCore`). Not alpha.60's `JazzSessionProvider`, whose name the
  // bindings keep as a removed-API stub.
  JazzSessionProvider as JazzHostSessionProvider,
  useJazzSession,
  reportJazzStartupFailure,
  type JazzSessionSourceProps,
  type JazzSessionViewProps,
  type UseJazzSessionResult,
} from "./session.js";

// Removed in alpha.61: stubs that throw, naming the replacement (one release).
export { createUseLocalFirstAuth, useJazzSessionOwner } from "../removed-api.js";
