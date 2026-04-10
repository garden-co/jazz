"use client";

import { app } from "../schema";
import { JazzProvider, useAll, useDb } from "jazz-tools/react";

export default function ClientTodo() {
  return (
    <JazzProvider
      config={{
        appId: process.env.NEXT_PUBLIC_JAZZ_APP_ID!,
        serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!,
        localAuthMode: "anonymous",
        driver: { type: "memory" },
      }}
    >
      <TodoForm />
      <TodoList />
    </JazzProvider>
  );
}

function TodoList() {
  const todos = useAll(app.todos) ?? [];
  return (
    <ul className="mt-4 space-y-1">
      {todos.length === 0 && <li className="text-sm text-foreground/30 italic">No todos yet.</li>}
      {todos.map((todo) => (
        <li key={todo.id} className="text-sm py-1.5 border-b border-foreground/5 last:border-0">
          {todo.title}
        </li>
      ))}
    </ul>
  );
}

function TodoForm() {
  const db = useDb();
  const handleSubmit = (e: React.SubmitEvent<HTMLFormElement>) => {
    e.preventDefault();
    const form = e.target as HTMLFormElement;
    const title = form.titleField.value.trim();
    if (!title) return;
    db.insert(app.todos, { title, done: false });
    form.reset();
  };
  return (
    <form onSubmit={handleSubmit} className="flex gap-2">
      <input
        name="titleField"
        type="text"
        placeholder="New todo…"
        className="flex-1 text-sm bg-transparent border border-foreground/15 rounded px-3 py-1.5 outline-none focus:border-foreground/40 placeholder:text-foreground/25"
      />
      <button
        type="submit"
        className="text-sm px-3 py-1.5 border border-foreground/15 rounded hover:bg-foreground/5 transition-colors cursor-pointer"
      >
        Add
      </button>
    </form>
  );
}
