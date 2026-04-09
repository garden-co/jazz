import { JazzProvider } from "jazz-tools/react";
import { TodoList } from "./TodoList.js";

// #region context-setup-react-minimal
export default function App() {
  return (
    <JazzProvider
      config={{
        appId: "my-todo-app",
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
        appId: "my-app",
        serverUrl: "https://my-jazz-server.example.com",
        runtimeSources: {
          baseUrl: "/assets/jazz/",
        },
      }}
      fallback={<p>Loading...</p>}
    >
      {/* ... */}
    </JazzProvider>
  );
}
// #endregion context-setup-react-runtime-sources
