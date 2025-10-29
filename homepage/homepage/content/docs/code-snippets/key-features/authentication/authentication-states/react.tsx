import * as React from "react";
import { JazzReactProvider } from "jazz-tools/react";

// #region Basic
import { useAgent, useIsAuthenticated } from "jazz-tools/react";

function AuthStateIndicator() {
  const agent = useAgent();
  const isAuthenticated = useIsAuthenticated();

  // Check if guest mode is enabled in JazzReactProvider
  const isGuest = agent.$type$ !== "Account";

  // Anonymous authentication: has an account but not fully authenticated
  const isAnonymous = agent.$type$ === "Account" && !isAuthenticated;
  return (
    <div>
      {isGuest && <span>Guest Mode</span>}
      {isAnonymous && <span>Anonymous Account</span>}
      {isAuthenticated && <span>Authenticated</span>}
    </div>
  );
}
// #endregion

function App() {
  return <div>Hello World</div>;
}
const apiKey = "you@example.com";

// #region SyncSettings
<JazzReactProvider
  sync={{
    peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
    // Controls when sync is enabled for
    // both Anonymous Authentication and Authenticated Account
    when: "always", // or "signedUp" or "never"
  }}
>
  <App />
</JazzReactProvider>;
// #endregion

// #region DisableAnonSync
<JazzReactProvider
  sync={{
    peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
    // This makes the app work in local mode when using Anonymous Authentication
    when: "signedUp",
  }}
>
  <App />
</JazzReactProvider>;
// #endregion

// #region GuestModeAccess
<JazzReactProvider
  // Enable Guest Mode for public content
  guestMode={true}
  sync={{
    peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
    // Only sync for authenticated users
    when: "signedUp",
  }}
>
  <App />
</JazzReactProvider>;
// #endregion
