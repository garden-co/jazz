// #region local-first-client-setup
import { JazzProvider, useLocalFirstAuth } from "jazz-tools/react";

function App() {
  const { secret, isLoading } = useLocalFirstAuth();

  if (isLoading || !secret) return <p>Loading…</p>;

  return (
    <JazzProvider
      config={{
        appId: "my-app",
        serverUrl: "wss://your-jazz-server.example.com",
        secret,
      }}
    >
      <YourApp />
    </JazzProvider>
  );
}
// #endregion local-first-client-setup

function YourApp() {
  return null;
}

export default App;
