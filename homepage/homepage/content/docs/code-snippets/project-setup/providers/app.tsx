import * as React from "react";
const apiKey = "you@example.com";
import { syncConfig } from "./sync-config";

// #region Basic
import { JazzReactProvider } from "jazz-tools/react";
import { MyAppAccount } from "./schema";

//@ts-expect-error redeclared
export function MyApp({ children }: { children: React.ReactNode }) {
  return (
    <JazzReactProvider
      sync={{
        peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
        when: "always", // When to sync: "always", "never", or "signedUp"
      }}
      AccountSchema={MyAppAccount}
    >
      {children}
    </JazzReactProvider>
  );
}
// #endregion

// #region AllOptions
//@ts-expect-error redeclared
export function MyApp({ children }: { children: React.ReactNode }) {
  return (
    <JazzReactProvider
      sync={syncConfig}
      // Enable guest mode for account-less access
      guestMode={false}
      // Enable SSR mode
      enableSSR={false}
      // Set default name for new user profiles
      defaultProfileName="New User"
      // Override the default storage key
      authSecretStorageKey="jazz-logged-in-secret"
      // Handle user logout
      onLogOut={() => {
        console.log("User logged out");
      }}
      // Handle anonymous account data when user logs in to existing account
      onAnonymousAccountDiscarded={(account) => {
        console.log("Anonymous account discarded", account.$jazz.id);
        // Migrate data here
        return Promise.resolve();
      }}
    >
      {children}
    </JazzReactProvider>
  );
}
// #endregion
