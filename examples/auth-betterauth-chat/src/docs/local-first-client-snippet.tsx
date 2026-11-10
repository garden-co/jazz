// #region local-first-client-setup
import { JazzSessionProvider } from "jazz-tools/react";

function App() {
  return (
    <JazzSessionProvider
      config={{
        // Your backend's Jazz endpoint; the browser names no app.
        serverUrl: "/jazz",
        initial: "local-first",
        // Local data, so a guest works offline from its first visit.
        client: () => import("jazz-tools/local-first"),
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
