import { JazzProvider } from "jazz-tools/react";
import { TodoList } from "./TodoList.js";

function readEnvAppId(): string | undefined {
  return (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env
    ?.JAZZ_APP_ID;
}

// #region context-setup-react
export function App() {
  return (
    <JazzProvider
      config={{
        appId: readEnvAppId() ?? "todo-react-example",
        env: "dev",
        userBranch: "main",
      }}
      fallback={<p>Loading...</p>}
    >
      <h1>Todos</h1>
      <TodoList />
    </JazzProvider>
  );
}
// #endregion context-setup-react
