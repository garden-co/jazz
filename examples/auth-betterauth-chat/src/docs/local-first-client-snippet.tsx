// #region local-first-client-setup
import { useEffect, useState } from "react";
import { createAccountManager, type AccountHandle } from "jazz-tools";
import { JazzProvider } from "jazz-tools/react";

const config = { appId: "my-app", serverUrl: "wss://your-jazz-server.example.com" };

function App() {
  const [account, setAccount] = useState<AccountHandle>();
  const [error, setError] = useState<string>();
  useEffect(() => {
    let cancelled = false;
    void createAccountManager(config)
      .then((accounts) => {
        if (!cancelled) setAccount(accounts.getLoggedIn() ?? accounts.createLocalFirst());
      })
      .catch((cause: unknown) => {
        if (!cancelled) setError(cause instanceof Error ? cause.message : String(cause));
      });
    return () => {
      cancelled = true;
    };
  }, []);
  if (error) return <p role="alert">{error}</p>;
  if (!account) return <p>Loading…</p>;
  return (
    <JazzProvider config={{ ...config, account }}>
      <YourApp />
    </JazzProvider>
  );
}
// #endregion local-first-client-setup

function YourApp() {
  return null;
}
export default App;
