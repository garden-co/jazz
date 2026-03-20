export { default as JazzSvelteProvider } from "./JazzSvelteProvider.svelte";
export {
  createJazzClient,
  createExtensionJazzClient,
  type JazzClient,
} from "./create-jazz-client.js";
export { getDb, getSession, getJazzContext, type JazzContext } from "./context.svelte.js";
export { QuerySubscription } from "./use-all.svelte.js";
export { default as SyntheticUserSwitcher } from "./SyntheticUserSwitcher.svelte";
export {
  useLinkExternalIdentity,
  type LinkExternalIdentityInput,
  type UseLinkExternalIdentityOptions,
} from "./use-link-external-identity.js";
export {
  createSyntheticUserProfile,
  getActiveSyntheticAuth,
  loadSyntheticUserStore,
  saveSyntheticUserStore,
  setActiveSyntheticProfile,
  syntheticUserStorageKey,
  type ActiveSyntheticAuth,
  type StorageLike,
  type SyntheticUserProfile,
  type SyntheticUserStorageOptions,
  type SyntheticUserStore,
} from "../synthetic-users.js";
export { attachDevTools, type DevToolsAttachment } from "../dev-tools/dev-tools.js";
export type { DurabilityTier, QueryOptions } from "../runtime/index.js";
