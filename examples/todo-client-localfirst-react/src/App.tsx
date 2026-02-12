import { JazzProvider, type JazzProviderProps } from "jazz-react";
import { TodoList } from "./TodoList.js";

export function App({ config, fallback }: Partial<JazzProviderProps> = {}) {
  return (
    <JazzProvider
      config={config ?? { appId: "todo-react-example", env: "dev", userBranch: "main" }}
      fallback={fallback ?? <p>Loading...</p>}
    >
      <h1>Todos</h1>
      <TodoList />
    </JazzProvider>
  );
}
