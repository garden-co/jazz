import { useState } from "react";
// #region reading-reactive-hooks-react
import { useDb, useAll } from "jazz-tools/react";
import { app } from "../schema.js";

export function TodoList() {
  // #region read-write-react
  const db = useDb();
  const { data: todos = [] } = useAll(app.todos);
  // #endregion reading-reactive-hooks-react
  // #region reading-filtering-react
  const { data: incompleteTodos } = useAll(
    app.todos.where({ done: false }).orderBy("title", "asc").limit(50),
  );
  // #endregion reading-filtering-react

  // #region where-subscription-react
  const pending = useAll(app.todos.where({ done: false }));
  // #endregion where-subscription-react

  // #region reading-tier-react
  const todosAtEdgeDurability = useAll(app.todos, { tier: "global" });
  // #endregion reading-tier-react

  // #region reading-loading-state-react
  const allTodos = useAll(app.todos);
  // `allTodos.data` may contain a local preview while `allTodos.isLoading` is `true`.
  // It is `undefined` only when no result is available yet.
  // Once the requested first result is ready, `allTodos.isLoading` is `false`.
  // A loaded empty query returns `[]`.
  // #endregion reading-loading-state-react

  // #region reading-conditional-query-react
  const [filter, setFilter] = useState<string | null>(null);
  const { data: filtered } = useAll(
    filter ? app.todos.where({ title: { contains: filter } }) : undefined,
  );
  // #endregion reading-conditional-query-react

  // #region writing-use-db-react
  function addTodo(todoTitle: string) {
    db.insert(app.todos, { title: todoTitle, done: false });
  }

  function toggleTodo(todo: { id: string; done: boolean }) {
    db.update(app.todos, todo.id, { done: !todo.done });
  }

  function removeTodo(id: string) {
    db.delete(app.todos, id);
  }
  // #endregion writing-use-db-react
  // #endregion read-write-react

  const [title, setTitle] = useState("");

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!title.trim()) return;
    addTodo(title.trim());
    setTitle("");
  };

  return (
    <>
      <span
        className={`settlement-badge settlement-badge--${allTodos.highestSettledAt}`}
        role="status"
        aria-label={`Query settlement: ${allTodos.highestSettledAt}`}
      >
        {allTodos.highestSettledAt === "unconfirmed"
          ? "Waiting"
          : allTodos.highestSettledAt === "local"
            ? "On device"
            : "Synced"}
      </span>
      <form onSubmit={handleSubmit}>
        <input
          type="text"
          value={title}
          onChange={(e) => setTitle(e.target.value)}
          placeholder="What needs to be done?"
          required
        />
        <button type="submit">Add</button>
      </form>
      <ul id="todo-list">
        {todos.map((todo) => (
          <li key={todo.id} className={todo.done ? "done" : ""}>
            <input
              type="checkbox"
              checked={todo.done}
              onChange={() => void toggleTodo(todo)}
              className="toggle"
            />
            <span>{todo.title}</span>
            {todo.description && <small>{todo.description}</small>}
            <button className="delete-btn" onClick={() => void removeTodo(todo.id)}>
              &times;
            </button>
          </li>
        ))}
      </ul>
    </>
  );
}
