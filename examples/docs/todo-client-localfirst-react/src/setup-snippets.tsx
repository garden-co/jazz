import { createJazzClient, JazzProvider } from "jazz-tools/react";
import { TodoList } from "./TodoList.js";

// #region context-setup-react-minimal
const client = createJazzClient({
  appId: "todo-react-example",
  serverUrl: "http://127.0.0.1:1625",
  localAuthMode: "anonymous",
  // jwtToken: authToken, // Use this (instead of localAuthMode) for external auth.
});

export function AppMinimal() {
  return (
    <JazzProvider client={client}>
      <h1>Todos</h1>
      <TodoList />
    </JazzProvider>
  );
}
// #endregion context-setup-react-minimal
