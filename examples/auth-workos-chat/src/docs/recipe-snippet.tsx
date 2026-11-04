import { JazzProvider, useJazzAuth } from "jazz-tools/react";
import { useWorkOSSession } from "../session.js";
function YourApp({ signOut }: { signOut: () => Promise<void> }) {
  const { reauthenticate } = useJazzAuth();
  return (
    <button
      onClick={async () => {
        await signOut();
        await reauthenticate();
      }}
    >
      Sign out
    </button>
  );
}

// #region workos-jazz-react
export function App() {
  // Your backend's own endpoint, reading the AuthKit session cookie.
  const { signIn, signOut } = useWorkOSSession();
  return (
    <JazzProvider
      appId="my-app"
      serverUrl="/jazz"
      signedOut={<button onClick={() => void signIn()}>Sign in</button>}
    >
      <YourApp signOut={signOut} />
    </JazzProvider>
  );
}
// Jazz asks your backend's `authenticate` who is signed in on start and on
// focus; call `reauthenticate()` from `useJazzAuth()` after your own sign-out.
// #endregion workos-jazz-react
