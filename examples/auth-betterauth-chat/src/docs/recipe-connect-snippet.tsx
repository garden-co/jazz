import {
  JazzSessionProvider,
  useJazzSessionOwner,
  useBetterAuth,
  type JazzSession,
  type JazzClient,
} from "jazz-tools/react";
import { authClient } from "../lib/auth-client";
function YourApp() {
  return null;
}

// #region betterauth-jazz-react
export function App() {
  const { session, error, retry } = useJazzSessionOwner({
    appId: "my-app",
    serverUrl: "wss://your-jazz-server.example.com",
  });
  if (error) return <button onClick={() => void retry()}>Retry: {error.message}</button>;
  if (!session) return <p>Loading...</p>;
  return <Connected session={session} />;
}
function Connected({ session }: { session: JazzSession<JazzClient> }) {
  const auth = useBetterAuth(session, authClient);
  // Forms only call authClient.signUp / signIn. Jazz follows automatically.
  const fallback = auth.error ? (
    <button onClick={() => void auth.retry()}>Retry: {auth.error.message}</button>
  ) : auth.isPending ? (
    <p>Loading...</p>
  ) : (
    <p>Sign in to continue.</p>
  );
  return (
    <JazzSessionProvider session={session} fallback={fallback}>
      {auth.ready ? (
        <>
          <button onClick={() => void auth.logout().catch(() => {})}>Sign out</button>
          <YourApp />
        </>
      ) : (
        fallback
      )}
    </JazzSessionProvider>
  );
}
// #endregion betterauth-jazz-react
