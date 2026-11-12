import { JazzProvider, useJazzSession } from "jazz-tools/react";
import { authClient } from "../lib/auth-client";
function YourApp() {
  const { refresh } = useJazzSession();
  async function signOut() {
    await authClient.signOut();
    await refresh();
  }
  return <button onClick={() => void signOut()}>Sign out</button>;
}

// #region betterauth-jazz-react
export function App() {
  return (
    <JazzProvider config={{ serverUrl: "/jazz" }} signedOut={<p>Sign in to continue.</p>}>
      <YourApp />
    </JazzProvider>
  );
}
// Your backend's `authenticate` decides who is signed in. After your forms
// call authClient.signUp / signIn / signOut, call `refresh()` from
// `useJazzSession()` and Jazz follows the backend's answer.
// #endregion betterauth-jazz-react
