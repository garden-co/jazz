import { createUseBetterAuthHook } from "jazz-auth-betterauth/react";
import {
  useAuthSecretStorage,
  useIsAuthenticated,
  useJazzContext,
} from "jazz-tools/react-native-core";

/**
 * React Native version of useBetterAuth
 * @category Auth Providers
 */
export const useBetterAuth = createUseBetterAuthHook({
  useJazzContext,
  useAuthSecretStorage,
  useIsAuthenticated,
});
