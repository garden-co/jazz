import { useEffect, useRef, useState } from "react";
import { AuthKitProvider, useAuth } from "@workos-inc/authkit-react";
import { createAccountManager, type AccountHandle } from "jazz-tools";
import { JazzProvider } from "jazz-tools/react";

function YourApp() {
  return null;
}

// #region workos-jazz-react
function JazzWithWorkOS() {
  const { user, getAccessToken } = useAuth();
  const [account, setAccount] = useState<AccountHandle>();
  const [error, setError] = useState<string>();
  const latestToken = useRef(getAccessToken);
  latestToken.current = getAccessToken;

  useEffect(() => {
    let cancelled = false;
    setAccount(undefined);
    if (!user) return;
    void createAccountManager({ appId: "my-app", serverUrl: "wss://your-jazz-server.example.com" })
      .then((accounts) => accounts.loginJWT({ getToken: () => latestToken.current() }))
      .then((handle) => {
        if (!cancelled) setAccount(handle);
      })
      .catch((cause: unknown) => {
        if (!cancelled) setError(cause instanceof Error ? cause.message : String(cause));
      });
    return () => {
      cancelled = true;
    };
  }, [user?.id]);
  // Registration is a separate explicit action for a fresh provider identity.
  if (error) return <p role="alert">{error}</p>;
  if (!account) return <p>Sign in to continue.</p>;

  return (
    <JazzProvider
      config={{
        appId: "my-app",
        serverUrl: "wss://your-jazz-server.example.com",
        account,
      }}
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
