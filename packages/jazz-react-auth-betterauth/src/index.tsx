import {
  createAuthContext,
  createUseBetterAuthHook,
} from "jazz-auth-betterauth/react";
import {
  useAuthSecretStorage,
  useIsAuthenticated,
  useJazzContext,
} from "jazz-tools/react";

export * from "./lib/social.js";
export * from "./types/auth.js";
export { AuthProvider, useAuth };

// Create the useBetterAuth hook using the factory with React hooks
export const useBetterAuth = createUseBetterAuthHook({
  useJazzContext,
  useAuthSecretStorage,
  useIsAuthenticated,
});

// Create and export the AuthContext, AuthProvider, and useAuth
const { AuthProvider, useAuth } = createAuthContext(useBetterAuth);
