// #region local-first-client-setup
import { JazzProvider } from "jazz-tools/react";

function App() {
  return (
    <JazzProvider
      config={{
        // Your backend's Jazz endpoint; the browser names no app.
        serverUrl: "/jazz",
        // Start every visitor as a guest on a device account.
        initial: "device",
        // Local data, so a guest works offline from its first visit.
        client: () => import("jazz-tools/local-first"),
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
