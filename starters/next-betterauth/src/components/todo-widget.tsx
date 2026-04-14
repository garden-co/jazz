"use client";

import { useState } from "react";
import { useDb, useAll } from "jazz-tools/react";
import { app } from "@/schema";

export function TodoWidget() {
  const db = useDb();
  const todos = useAll(app.todos) ?? [];
  const [title, setTitle] = useState("");

  function add(e: React.FormEvent) {
    e.preventDefault();
    const trimmed = title.trim();
    if (!trimmed) return;
    db.insert(app.todos, { title: trimmed, done: false });
    setTitle("");
  }

  return (
    <section className="todo-widget">
      <h2>Your todos</h2>
      <form onSubmit={add}>
        <input
          type="text"
          value={title}
          onChange={(e) => setTitle(e.target.value)}
          placeholder="Add a task"
          aria-label="New todo"
        />
        <button type="submit">Add</button>
      </form>
      <ul>
        {todos.map((t) => (
          <li key={t.id} className={t.done ? "done" : ""}>
            <label>
              <input
                type="checkbox"
                checked={t.done}
                onChange={() => db.update(app.todos, t.id, { done: !t.done })}
              />
              <span>{t.title}</span>
            </label>
            <button type="button" aria-label="Delete" onClick={() => db.delete(app.todos, t.id)}>
              ×
            </button>
          </li>
        ))}
      </ul>
    </section>
  );
}
