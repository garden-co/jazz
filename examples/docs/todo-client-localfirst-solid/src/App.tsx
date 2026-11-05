import { JazzProvider, type JazzAppConfig } from "jazz-tools/solid";
import { TodoList } from "./TodoList.js";

export function App(props: { config: JazzAppConfig }) {
  return (
    <JazzProvider initial="local-first" {...props.config}>
      <h1>Todos</h1>
      <TodoList />
    </JazzProvider>
  );
}
