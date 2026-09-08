import { JazzProvider, betterAuth, useJazzAuth } from "jazz-tools/react";
import { authClient } from "../lib/auth-client";
function YourApp() {
  const { logout } = useJazzAuth();
  return <button onClick={() => void logout()}>Sign out</button>;
}

// #region betterauth-jazz-react
export function App() {
  return (
    <JazzProvider
      appId="my-app"
      serverUrl="wss://your-jazz-server.example.com"
      auth={betterAuth(authClient)}
      signedOut={<p>Sign in to continue.</p>}
    >
      <YourApp />
    </JazzProvider>
  );
}
// Forms only call authClient.signUp / signIn. Jazz follows automatically.
// #endregion betterauth-jazz-react
