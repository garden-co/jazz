import { JazzProvider, createSolidJazzClient } from "jazz-tools/solid";
import { TodoList } from "./TodoList.js";

export function App() {
  const client = createSolidJazzClient(() => ({ appId: "<your-app-id>" }));

  return (
    <JazzProvider client={client} fallback={<p>Loading...</p>}>
      <h1>Todos</h1>
      <TodoList />
    </JazzProvider>
  );
}
