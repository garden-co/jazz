export { default as JazzSvelteProvider } from "./JazzSvelteProvider.svelte";
export { default as JazzSvelteClientProvider } from "./JazzSvelteClientProvider.svelte";
export { createJazzClient, type JazzClient } from "./create-jazz-client.js";
export { getDb, getSession, getJazzContext, type JazzContext } from "./context.svelte.js";
export { QuerySubscription, QuerySubscriptionOne } from "./query-subscription.svelte.js";
export { LocalFirstAuth } from "./local-first-auth.svelte.js";
export type { DurabilityTier, QueryOptions, RuntimeSourcesConfig } from "../runtime/index.js";
export {
  BrowserAuthSecretStore,
  generateAuthSecret,
  type AuthSecretStore,
  type BrowserAuthSecretStoreOptions,
} from "../runtime/auth-secret-store.js";

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

export { accountState } from "./account-state.js";

export { createJazzSession, type JazzSessionConfig } from "../session/create-jazz-session.js";
export type { JazzSession, JazzSessionSnapshot } from "../session/state.js";
export { default as JazzSessionProvider } from "./JazzSessionProvider.svelte";
export { sessionState, getJazzSession, type JazzSessionState } from "./session-state.js";

export {
  connectAuthProvider,
  type AuthProviderConnection,
  type AuthProviderState,
  type AuthProviderSnapshot,
} from "../session/auth-provider.js";
export { connectBetterAuth, type BetterAuthClient } from "../session/better-auth.js";
