import { ClerkLoaded, ClerkProvider, useClerk } from "@clerk/clerk-expo";
import { resourceCache } from "@clerk/clerk-expo/resource-cache";
import { tokenCache } from "@clerk/clerk-expo/token-cache";
import { JazzExpoProviderWithClerk } from "jazz-tools/expo";
import { apiKey } from "./apiKey";
import { AuthScreen } from "./auth/AuthScreen";
import { MainScreen } from "./MainScreen";

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
      __experimental_resourceCache={resourceCache}
      publishableKey={publishableKey}
    >
      <ClerkLoaded>
        <JazzAndAuth>
          <AuthScreen>
            <MainScreen />
          </AuthScreen>
        </JazzAndAuth>
      </ClerkLoaded>
    </ClerkProvider>
  );
}
