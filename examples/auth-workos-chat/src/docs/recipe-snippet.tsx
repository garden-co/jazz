import { JazzProvider, useJazzSession } from "jazz-tools/react";
import { useWorkOSSession } from "../session.js";
function YourApp({ signOut }: { signOut: () => Promise<void> }) {
  const { refresh } = useJazzSession();
  return (
    <button
      onClick={async () => {
        await signOut();
        await refresh();
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
      config={{ serverUrl: "/jazz" }}
      signedOut={<button onClick={() => void signIn()}>Sign in</button>}
    >
      <YourApp signOut={signOut} />
    </JazzProvider>
  );
}
// Jazz asks your backend's `authenticate` who is signed in on start and on
// focus; call `refresh()` from `useJazzSession()` after your own sign-out.
// #endregion workos-jazz-react
