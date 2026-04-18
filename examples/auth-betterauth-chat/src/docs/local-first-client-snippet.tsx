// #region local-first-client-setup
import * as React from "react";
import { JazzProvider, useLocalFirstAuth } from "jazz-tools/react";

function App() {
  return (
    <React.Suspense fallback={<p>Loading…</p>}>
      <AppInner />
    </React.Suspense>
  );
}

function AppInner() {
  const auth = useLocalFirstAuth();
  const secret = React.use(auth.getOrCreateSecret());

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
