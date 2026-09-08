import { AuthKitProvider, useAuth } from "@workos-inc/authkit-react";
import {
  JazzSessionProvider,
  useJazzSessionOwner,
  useAuthProvider,
  type JazzSession,
  type JazzClient,
} from "jazz-tools/react";
function YourApp() {
  return null;
}

// #region workos-jazz-react
function JazzWithWorkOS() {
  const { session, error, retry } = useJazzSessionOwner({
    appId: "my-app",
    serverUrl: "wss://your-jazz-server.example.com",
  });
  if (error) return <button onClick={() => void retry()}>Retry: {error.message}</button>;
  if (!session) return <p>Loading...</p>;
  return <WorkOSSession session={session} />;
}
function WorkOSSession({ session }: { session: JazzSession<JazzClient> }) {
  const { user, isLoading, getAccessToken, signIn, signOut } = useAuth();
  const auth = useAuthProvider(
    session,
    { key: user?.id ?? null, isPending: isLoading },
    {
      getToken: () => getAccessToken({ forceRefresh: true }),
    },
  );
  const fallback = auth.error ? (
    <button onClick={() => void auth.retry()}>Retry: {auth.error.message}</button>
  ) : auth.isPending ? (
    <p>Loading...</p>
  ) : (
    <button onClick={() => void signIn()}>Sign in</button>
  );
  return (
    <JazzSessionProvider session={session} fallback={fallback}>
      {auth.ready && user ? (
        <>
          <button onClick={() => void auth.logout(() => signOut()).catch(() => {})}>
            Sign out
          </button>
          <YourApp />
        </>
      ) : (
        fallback
      )}
    </JazzSessionProvider>
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
