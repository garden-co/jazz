export { CoState, AccountCoState, InviteListener, SyncConnectionStatus } from "../classic-api.js";
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
export * from "../accounts/exports.js";

export { accountState } from "./account-state.js";
export { syncStatus } from "./sync-status.js";
export type { LinkStatus, SyncStatus } from "../runtime/sync-status.js";

export { createJazzSession, type JazzSessionConfig } from "../session/create-jazz-session.js";
export type { JazzSession, JazzSessionSnapshot } from "../session/state.js";
export { default as JazzSessionProvider } from "./JazzSessionProvider.svelte";
export { sessionState, getJazzSession, type JazzSessionState } from "./session-state.js";

export * from "../session/auth-provider-exports.js";

export { default as JazzProvider } from "./JazzProvider.svelte";
export { useJazzAuth, type JazzAuthState } from "./auth-state.js";
export { betterAuth, jwtAuth, type JazzAuth, type JazzAppSnapshot } from "../session/app.js";
export type { JazzAppConfig } from "../session/create-jazz-app.js";
