import { useState } from "react";
import { useDb, useAll, useSession } from "jazz-tools/react";
import { app } from "../schema/app.js";

export function TodoList() {
  const [filterTitle, setFilterTitle] = useState("");
  const [showDoneOnly, setShowDoneOnly] = useState(false);
  const trimmedFilterTitle = filterTitle.trim();
  let todosQuery = app.todos;
  if (trimmedFilterTitle) {
    todosQuery = todosQuery.where({ title: { contains: trimmedFilterTitle } });
  }
  if (showDoneOnly) {
    todosQuery = todosQuery.where({ done: true });
  }

  // #region reading-reactive-hooks-react
  const db = useDb();
  const todos = useAll(todosQuery) ?? [];
  // #endregion reading-reactive-hooks-react
  const session = useSession();
  const sessionUserId = session?.user_id ?? null;
  const [title, setTitle] = useState("");

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
      <div>
        <input
          type="text"
          value={filterTitle}
          onChange={(e) => setFilterTitle(e.target.value)}
          placeholder="Filter by title (contains)"
          aria-label="Filter by title"
        />
        <label>
          <input
            type="checkbox"
            checked={showDoneOnly}
            onChange={(e) => setShowDoneOnly(e.target.checked)}
          />
          Done only
        </label>
      </div>
      <ul id="todo-list">
        {todos.map((todo) => (
          <li key={todo.id} className={todo.done ? "done" : ""}>
            <input
              type="checkbox"
              checked={todo.done}
              onChange={() => db.update(app.todos, todo.id, { done: !todo.done })}
              className="toggle"
            />
            <span>{todo.title}</span>
            {todo.description && <small>{todo.description}</small>}
            <button className="delete-btn" onClick={() => db.delete(app.todos, todo.id)}>
              &times;
            </button>
          </li>
        ))}
      </ul>
    </>
  );
}
