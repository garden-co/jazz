import * as React from "react";

/**
 * Use your email as a temporary key, or get a free
 * API Key at dashboard.jazz.tools for higher limits.
 *
 * @link https://dashboard.jazz.tools
 */
const apiKey = "you@example.com";

const tokenCache = {
  getToken: async (key: string) => {
    return null;
  },
  saveToken: async (key: string, token: string) => {},
  clearToken: async (key: string) => {},
};
function MainScreen() {
  return null;
}
// #region Basic
import { useClerk, ClerkProvider, ClerkLoaded } from "@clerk/clerk-expo";
import { resourceCache } from "@clerk/clerk-expo/resource-cache";
import { JazzExpoProviderWithClerk } from "jazz-tools/expo";

function JazzAndAuth({ children }: { children: React.ReactNode }) {
  const clerk = useClerk();

  return (
    <JazzExpoProviderWithClerk
      clerk={clerk}
      sync={{
        peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
      }}
    >
      {children}
    </JazzExpoProviderWithClerk>
  );
}

export default function RootLayout() {
  const publishableKey = process.env.EXPO_PUBLIC_CLERK_PUBLISHABLE_KEY;

  if (!publishableKey) {
    throw new Error(
      "Missing Publishable Key. Please set EXPO_PUBLIC_CLERK_PUBLISHABLE_KEY in your .env",
    );
  }

  return (
    <ClerkProvider
      tokenCache={tokenCache}
      publishableKey={publishableKey}
      __experimental_resourceCache={resourceCache}
    >
      <ClerkLoaded>
        <JazzAndAuth>
          <MainScreen />
        </JazzAndAuth>
      </ClerkLoaded>
    </ClerkProvider>
  );
}
// #endregion

// #region AuthButton
import { useSignIn } from "@clerk/clerk-expo";
import { useIsAuthenticated, useLogOut } from "jazz-tools/expo";
import { Button, Text } from "react-native";

export function AuthButton() {
  const logOut = useLogOut();
  const { signIn, setActive, isLoaded } = useSignIn();
  const isAuthenticated = useIsAuthenticated();

  if (isAuthenticated) {
    return <Button title="Logout" onPress={() => logOut()} />;
  }

  const onSignInPress = async () => {
    if (!isLoaded) return;
    const signInAttempt = await signIn.create({
      identifier: "you@example.com",
      password: "password",
    });
    if (signInAttempt.status === "complete") {
      await setActive({ session: signInAttempt.createdSessionId });
    }
  };

  return <Button title="Sign In" onPress={onSignInPress} />;
}
// #endregion
