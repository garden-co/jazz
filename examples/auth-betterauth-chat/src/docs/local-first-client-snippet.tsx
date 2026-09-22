// #region local-first-client-setup
import { JazzSessionProvider } from "jazz-tools/react";

function App() {
  return (
    <JazzSessionProvider
      config={{
        appId: "my-app",
        serverUrl: "wss://your-jazz-server.example.com",
        initial: "local-first",
      }}
    >
      <YourApp />
    </JazzSessionProvider>
  );
}
// #endregion local-first-client-setup

function YourApp() {
  return null;
}
export default App;
