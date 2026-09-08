import { AuthKitProvider, useAuth } from "@workos-inc/authkit-react";
import { JazzProvider, jwtAuth, useJazzAuth } from "jazz-tools/react";
function YourApp() {
  const { logout } = useJazzAuth();
  return <button onClick={() => void logout()}>Sign out</button>;
}

// #region workos-jazz-react
function JazzWithWorkOS() {
  const { user, isLoading, getAccessToken, signIn, signOut } = useAuth();
  return (
    <JazzProvider
      appId="my-app"
      serverUrl="wss://your-jazz-server.example.com"
      auth={jwtAuth({
        key: user?.id ?? null,
        isPending: isLoading,
        getToken: () => getAccessToken({ forceRefresh: true }),
        logout: () => signOut(),
      })}
      signedOut={<button onClick={() => void signIn()}>Sign in</button>}
    >
      <YourApp />
    </JazzProvider>
  );
}
export function App() {
  return (
    <AuthKitProvider clientId="client_01ABC...">
      <JazzWithWorkOS />
    </AuthKitProvider>
  );
}
// #endregion workos-jazz-react
