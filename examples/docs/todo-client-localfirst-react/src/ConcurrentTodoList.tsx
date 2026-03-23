import { Suspense } from "react";
import { useAllSuspense } from "jazz-tools/react";
import { app } from "../schema.js";

// #region reading-concurrent-rendering-react
function TodoList() {
  const query = app.todos.orderBy("title");

  return (
    <Suspense fallback={<p>Loading...</p>}>
      <TodoResults query={query} />
    </Suspense>
  );
}

function TodoResults({ query }: { query: ReturnType<typeof app.todos.orderBy> }) {
  const todos = useAllSuspense(query);

  return (
    <ul>
      {todos.map((todo) => (
        <li key={todo.id}>{todo.title}</li>
      ))}
    </ul>
  );
}
// #endregion reading-concurrent-rendering-react

export { TodoList as ConcurrentTodoList };
