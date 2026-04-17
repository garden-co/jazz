// #region local-first-client-setup
import * as React from "react";
import { JazzProvider, useLocalFirstAuth } from "jazz-tools/react";

function App() {
  const auth = useLocalFirstAuth();
  return (
    <React.Suspense fallback={<p>Loading…</p>}>
      <AppInner auth={auth} />
    </React.Suspense>
  );
}

function AppInner({ auth }: { auth: ReturnType<typeof useLocalFirstAuth> }) {
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
