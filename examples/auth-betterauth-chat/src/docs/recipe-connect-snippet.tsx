import { JazzProvider, useJazzAuth } from "jazz-tools/react";
import { authClient } from "../lib/auth-client";
function YourApp() {
  const { reauthenticate } = useJazzAuth();
  async function signOut() {
    await authClient.signOut();
    await reauthenticate();
  }
  return <button onClick={() => void signOut()}>Sign out</button>;
}

// #region betterauth-jazz-react
export function App() {
  return (
    <JazzProvider appId="my-app" serverUrl="/jazz" signedOut={<p>Sign in to continue.</p>}>
      <YourApp />
    </JazzProvider>
  );
}
// Your backend's `authenticate` decides who is signed in. After your forms
// call authClient.signUp / signIn / signOut, call `reauthenticate()` from
// `useJazzAuth()` and Jazz follows the backend's answer.
// #endregion betterauth-jazz-react
