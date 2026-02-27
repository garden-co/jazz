import { createJazzClient, JazzProvider } from "jazz-tools/react";
import { TodoList } from "./TodoList.js";

// #region context-setup-react-minimal
const client = createJazzClient({
  appId: "todo-react-example",
  env: "dev",
  userBranch: "main",
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
