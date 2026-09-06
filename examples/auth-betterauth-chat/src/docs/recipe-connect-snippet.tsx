import { useEffect, useState } from "react";
import { createAccountManager, type AccountHandle } from "jazz-tools";
import { JazzProvider } from "jazz-tools/react";
import { authClient } from "../lib/auth-client";

function YourApp() {
  return null;
}

// #region betterauth-jazz-react
// Mount after provider sign-in. Login requires a previously registered/linked
// identity; registration is a separate explicit application action.
export function App() {
  const [account, setAccount] = useState<AccountHandle>();
  const [error, setError] = useState<string>();
  const config = { appId: "my-app", serverUrl: "wss://your-jazz-server.example.com" };
  useEffect(() => {
    let cancelled = false;
    void (async () => {
      const accounts = await createAccountManager(config);
      const handle = await accounts.loginJWT({
        getToken: async () => {
          const result = await authClient.token();
          if (result.error) throw new Error(result.error.message);
          return result.data.token;
        },
      });
      if (!cancelled) setAccount(handle);
    })().catch((cause: unknown) => {
      if (!cancelled) setError(cause instanceof Error ? cause.message : String(cause));
    });
    return () => {
      cancelled = true;
    };
  }, []);
  if (error) return <p role="alert">{error}</p>;
  if (!account) return <p>Resolving account…</p>;
  return (
    <JazzProvider config={{ ...config, account }}>
      <YourApp />
    </JazzProvider>
  );
}
// #endregion betterauth-jazz-react
