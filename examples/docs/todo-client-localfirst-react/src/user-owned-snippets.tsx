import { useAll, useDb } from "jazz-tools/react";
import { app } from "./recipes/user-owned/jazz.generated.js";

// #region owned-query
export function MyTodos() {
  const { data: todos, isLoading, error } = useAll(app.todos.where({ done: false }));

  if (isLoading) return <p>Loading…</p>;
  if (error) return <p>Something went wrong!</p>;

  return (
    <ul>
      {todos.map((todo) => (
        <li key={todo.id}>{todo.title}</li>
      ))}
    </ul>
  );
}
// #endregion owned-query

// #region owned-insert
export function AddTodo() {
  const db = useDb();

  function handleAdd(title: string) {
    db.insert(app.todos, { title, done: false });
  }

  return <button onClick={() => handleAdd("Buy milk")}>Add</button>;
}
// #endregion owned-insert
