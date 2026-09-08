// #region context-setup-react-minimal
import type { AccountHandle } from "jazz-tools";
import { JazzProvider } from "jazz-tools/react";
import { TodoList } from "./TodoList.js";

// Prepare the account outside the context with createAccountManager.
export default function App({ account }: { account: AccountHandle }) {
  return (
    <JazzProvider
      config={{
        appId: "<your-app-id>",
        account,
      }}
    >
      <h1>Todos</h1>
      <TodoList />
    </JazzProvider>
  );
}
// #endregion context-setup-react-minimal

// #region context-setup-react-runtime-sources
// Prepare this handle with the same runtimeSources and registry authority.
export function AppWithRuntimeSources({ account }: { account: AccountHandle }) {
  return (
    <JazzProvider
      config={{
        appId: "my-app",
        account,
        serverUrl: "https://my-jazz-server.example.com",
        runtimeSources: {
          baseUrl: "/assets/jazz/",
          wasmVersion: "2026-08-25", // Change this for every deployed asset build.
        },
      }}
      fallback={<p>Loading...</p>}
    >
      {/* Your app's main component */}
      <TodoList />
    </JazzProvider>
  );
}
// #endregion context-setup-react-runtime-sources
