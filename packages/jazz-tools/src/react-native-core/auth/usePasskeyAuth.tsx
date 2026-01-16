import {
  useAuthSecretStorage,
  useIsAuthenticated,
  useJazzContextValue,
} from "jazz-tools/react-core";
import { useMemo } from "react";
import { ReactNativePasskeyAuth } from "./PasskeyAuth.js";

/**
 * React hook for passkey (WebAuthn) authentication in React Native apps.
 *
 * This hook provides a simple interface for signing up and logging in with passkeys,
 * using the device's biometric authentication (FaceID/TouchID/fingerprint).
 *
 * **Requirements:**
 * - Install `react-native-passkey` as a peer dependency
 * - Configure your app's associated domains (iOS) and asset links (Android)
 * - Passkeys require HTTPS domain verification
 *
 * @example
 * ```tsx
 * import { usePasskeyAuth } from "jazz-tools/react-native-core";
 *
 * function AuthScreen() {
 *   const auth = usePasskeyAuth({
 *     appName: "My App",
 *     rpId: "myapp.com",
 *   });
 *
 *   if (auth.state === "signedIn") {
 *     return <MainApp />;
 *   }
 *
 *   return (
 *     <View>
 *       <Button title="Sign Up" onPress={() => auth.signUp("John Doe")} />
 *       <Button title="Log In" onPress={auth.logIn} />
 *     </View>
 *   );
 * }
 * ```
 *
 * @param options.appName - The display name of your app shown during passkey prompts
 * @param options.rpId - The relying party ID (your app's domain, e.g., "myapp.com")
 *
 * @category Auth Providers
 */
export function usePasskeyAuth({
  appName,
  rpId,
}: {
  appName: string;
  rpId: string;
}) {
  const context = useJazzContextValue();
  const authSecretStorage = useAuthSecretStorage();

  if ("guest" in context) {
    throw new Error("Passkey auth is not supported in guest mode");
  }

  const authMethod = useMemo(() => {
    return new ReactNativePasskeyAuth(
      context.node.crypto,
      context.authenticate,
      authSecretStorage,
      appName,
      rpId,
    );
  }, [
    appName,
    rpId,
    authSecretStorage,
    context.node.crypto,
    context.authenticate,
  ]);

  const isAuthenticated = useIsAuthenticated();

  return {
    state: isAuthenticated ? "signedIn" : "anonymous",
    logIn: authMethod.logIn,
    signUp: authMethod.signUp,
  } as const;
}
