// Auth-provider exports shared by the package root and the web entry points.
export {
  connectAuthProvider,
  type AuthProviderConnection,
  type AuthProviderState,
  type AuthProviderSnapshot,
} from "./auth-provider.js";
export { connectBetterAuth, type BetterAuthClient } from "./better-auth.js";
export { backendToken } from "./backend-token.js";
