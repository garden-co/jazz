// #region context-setup-react-minimal
import { JazzProvider } from "jazz-tools/react";
import { TodoList } from "./TodoList.js";

export default function App() {
  return (
    <JazzProvider
      config={{
        appId: "my-app", // Register for an app ID free at v2.dashboard.jazz.tools
      }}
    >
      <h1>Todos</h1>
      <TodoList />
    </JazzProvider>
  );
}
// #endregion context-setup-react-minimal

// #region context-setup-react-runtime-sources
export function AppWithRuntimeSources() {
  return (
    <JazzProvider
      config={{
        appId: "my-app", // Register for an app ID free at v2.dashboard.jazz.tools
        serverUrl: "https://my-jazz-server.example.com",
        runtimeSources: {
          baseUrl: "/assets/jazz/",
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
