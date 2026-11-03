import { JazzProvider, backendAuth, useJazzAuth } from "jazz-tools/react";
import { useWorkOSSession } from "../session.js";
function YourApp() {
  const { logout } = useJazzAuth();
  return <button onClick={() => void logout()}>Sign out</button>;
}

// #region workos-jazz-react
export function App() {
  // Your backend's own endpoint, reading the AuthKit session cookie.
  const { user, isLoading, signIn, signOut } = useWorkOSSession();
  return (
    <JazzProvider
      appId="my-app"
      serverUrl="/jazz"
      auth={backendAuth({ key: user?.email ?? null, isPending: isLoading, logout: signOut })}
      signedOut={<button onClick={() => void signIn()}>Sign in</button>}
    >
      <YourApp />
    </JazzProvider>
  );
}
// #endregion workos-jazz-react
