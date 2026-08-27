// #region context-setup-react-minimal
import { JazzProvider } from "jazz-tools/react";
import { TodoList } from "./TodoList.js";

export default function App() {
  return (
    <JazzProvider
      config={{
        appId: "<your-app-id>",
      }}
      auth="local-first"
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
        appId: "my-app",
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
