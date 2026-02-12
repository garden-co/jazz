import { JazzProvider } from "jazz-react";
import { TodoList } from "./TodoList.js";

export function App() {
  return (
    <JazzProvider
      config={{ appId: "todo-react-example", env: "dev", userBranch: "main" }}
      fallback={<p>Loading...</p>}
    >
      <h1>Todos</h1>
      <TodoList />
    </JazzProvider>
  );
}
