import { JazzProvider } from "jazz-tools/solid";
import { TodoList } from "./TodoList.js";

export function App() {
  return (
    <JazzProvider config={{ appId: "<your-app-id>" }} fallback={<p>Loading...</p>}>
      <h1>Todos</h1>
      <TodoList />
    </JazzProvider>
  );
}
