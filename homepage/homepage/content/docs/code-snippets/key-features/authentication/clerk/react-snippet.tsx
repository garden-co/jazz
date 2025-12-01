import * as React from "react";
import { createRoot } from "react-dom/client";

/**
 * Use your email as a temporary key, or get a free
 * API Key at dashboard.jazz.tools for higher limits.
 *
 * @link https://dashboard.jazz.tools
 */
const apiKey = "you@example.com";

const PUBLISHABLE_KEY = "fake_key";
function App() {
  return <div>Hello World</div>;
}
// #region Basic
import { useClerk, ClerkProvider } from "@clerk/clerk-react";
import { JazzReactProviderWithClerk } from "jazz-tools/react";

function JazzProvider({ children }: { children: React.ReactNode }) {
  const clerk = useClerk();

  return (
    <JazzReactProviderWithClerk
      clerk={clerk}
      sync={{
        peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
      }}
    >
      {children}
    </JazzReactProviderWithClerk>
  );
}

createRoot(document.getElementById("root")!).render(
  <ClerkProvider publishableKey={PUBLISHABLE_KEY} afterSignOutUrl="/">
    <JazzProvider>
      <App />
    </JazzProvider>
  </ClerkProvider>,
);
// #endregion Basic

// #region AuthButton
import { SignInButton } from "@clerk/clerk-react";
import { useIsAuthenticated, useLogOut } from "jazz-tools/react";

export function AuthButton() {
  const logOut = useLogOut();

  const isAuthenticated = useIsAuthenticated();

  if (isAuthenticated) {
    return <button onClick={() => logOut()}>Logout</button>;
  }

  return <SignInButton />;
}
// #endregion
