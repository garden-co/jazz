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
