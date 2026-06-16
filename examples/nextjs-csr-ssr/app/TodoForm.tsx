"use client";

import { useDb } from "jazz-tools/react";
import { app } from "../schema";

export default function TodoForm() {
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
