// #region local-first-client-setup
import { use } from "react";
import { BrowserAuthSecretStore } from "jazz-tools";
import { JazzProvider } from "jazz-tools/react";

function App() {
  const secret = use(BrowserAuthSecretStore.getOrCreateSecret());

  return (
    <JazzProvider
      config={{
        appId: "my-app",
        serverUrl: "wss://your-jazz-server.example.com",
        auth: { localFirstSecret: secret },
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
