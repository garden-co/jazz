import type { DbConfig } from "jazz-tools";
import { JazzProvider } from "jazz-tools/solid";
import { TodoList } from "./TodoList.js";

export function App(props: { config: DbConfig }) {
  return (
    <JazzProvider config={props.config} fallback={<p>Loading...</p>}>
      <h1>Todos</h1>
      <TodoList />
    </JazzProvider>
  );
}
