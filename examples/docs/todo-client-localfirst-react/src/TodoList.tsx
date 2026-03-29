import { useState } from "react";
import { useDb, useAll } from "jazz-tools/react";
import { app } from "../schema.js";

export function TodoList() {
  // #region read-write-react
  // #region reading-reactive-hooks-react
  const db = useDb();
  const todos = useAll(app.todos) ?? [];
  // #endregion reading-reactive-hooks-react

  // #region reading-filtering-react
  const _incompleteTodos = useAll(
    app.todos.where({ done: false }).orderBy("title", "asc").limit(50),
  );
  // #endregion reading-filtering-react

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
