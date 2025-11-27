import * as React from "react";
import { JazzReactNativeProvider } from "jazz-tools/react-native";
import { View, Text } from "react-native";

const App = () => {
  return <View></View>;
};

// #region Basic
import { useAgent, useIsAuthenticated } from "jazz-tools/react-native";

function AuthStateIndicator() {
  const agent = useAgent();
  const isAuthenticated = useIsAuthenticated();

  // Check if guest mode is enabled in JazzReactNativeProvider
  const isGuest = agent.$type$ !== "Account";

  // Anonymous authentication: has an account but not fully authenticated
  const isAnonymous = agent.$type$ === "Account" && !isAuthenticated;
  return (
    <View>
      {isGuest && <Text>Guest Mode</Text>}
      {isAnonymous && <Text>Anonymous Account</Text>}
      {isAuthenticated && <Text>Authenticated</Text>}
    </View>
  );
}
// #endregion
const apiKey = "you@example.com";

function example1() {
  // Don't auto indent
  // prettier-ignore
  return (
// #region SyncSettings
<JazzReactNativeProvider
  sync={{
    peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
    // Controls when sync is enabled for
    // both Anonymous Authentication and Authenticated Account
    when: "always", // or "signedUp" or "never"
  }}
>
  <App />
</JazzReactNativeProvider>
// #endregion
  );
}

function example2() {
  // Don't auto indent
  // prettier-ignore
  return (
<>
{/* #region DisableAnonSync */ }
<JazzReactNativeProvider
  sync={{
    peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
    // This makes the app work in local mode when using Anonymous Authentication
    when: "signedUp",
  }}
>
  <App />
</JazzReactNativeProvider>
{/* #endregion */ }
</>
);
}

function example3() {
  // Don't auto indent
  // prettier-ignore
  return (
// #region GuestModeAccess
<JazzReactNativeProvider
  // Enable Guest Mode for public content
  guestMode={true}
  sync={{
    peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
    // Only sync for authenticated users
    when: "signedUp",
  }}
>
  <App />
</JazzReactNativeProvider>
// #endregion
  );
}
