import { useState } from "react";
import { useDb, useAll, useSession } from "jazz-tools/react";
import { app, type Todo } from "../schema/app.js";

export function TodoList() {
  // #region reading-reactive-hooks-react
  const db = useDb();
  const todos = useAll(app.todos);
  // #endregion reading-reactive-hooks-react
  const session = useSession();
  const sessionUserId = session?.user_id ?? null;
  const [title, setTitle] = useState("");

  const canMutateTodo = (todo: Todo): boolean =>
    sessionUserId !== null && todo.owner_id === sessionUserId;

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!title.trim() || !sessionUserId) return;
    db.insert(app.todos, { title: title.trim(), done: false, owner_id: sessionUserId });
    setTitle("");
  };

  return (
    <>
      <form onSubmit={handleSubmit}>
        <input
          type="text"
          value={title}
          onChange={(e) => setTitle(e.target.value)}
          placeholder="What needs to be done?"
          required
        />
        <button type="submit" disabled={!sessionUserId}>
          Add
        </button>
      </form>
      <ul id="todo-list">
        {todos.map((todo) => (
          <li key={todo.id} className={todo.done ? "done" : ""}>
            <input
              type="checkbox"
              checked={todo.done}
              disabled={!canMutateTodo(todo)}
              onChange={() => {
                if (!canMutateTodo(todo)) return;
                db.update(app.todos, todo.id, { done: !todo.done });
              }}
              className="toggle"
            />
            <span>{todo.title}</span>
            {todo.description && <small>{todo.description}</small>}
            <button
              className="delete-btn"
              disabled={!canMutateTodo(todo)}
              onClick={() => {
                if (!canMutateTodo(todo)) return;
                db.deleteFrom(app.todos, todo.id);
              }}
            >
              &times;
            </button>
          </li>
        ))}
      </ul>
    </>
  );
}
