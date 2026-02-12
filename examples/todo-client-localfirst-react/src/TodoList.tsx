import { useState } from "react";
import { useDb, useAll } from "jazz-react";
import { app } from "../schema/app.js";

export function TodoList() {
  const db = useDb();
  const todos = useAll(app.todos);
  const [title, setTitle] = useState("");

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!title.trim()) return;
    db.insert(app.todos, { title: title.trim(), done: false });
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
              onChange={() => db.update(app.todos, todo.id, { done: !todo.done })}
              className="toggle"
            />
            <span>{todo.title}</span>
            {todo.description && <small>{todo.description}</small>}
            <button className="delete-btn" onClick={() => db.deleteFrom(app.todos, todo.id)}>
              &times;
            </button>
          </li>
        ))}
      </ul>
    </>
  );
}
