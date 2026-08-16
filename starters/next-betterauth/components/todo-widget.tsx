"use client";

import { useRef, useState } from "react";
import { useDb, useAll } from "jazz-tools/react";
import { app } from "@/schema";

export function TodoWidget() {
  const db = useDb();
  const { data: todos = [] } = useAll(app.todos);
  const [localSaveState, setLocalSaveState] = useState("Ready to save locally");
  const latestSaveGeneration = useRef(0);
  const pendingSaveCount = useRef(0);
  const latestSaveFailed = useRef(false);

  function renderLocalSaveState() {
    setLocalSaveState(
      latestSaveFailed.current
        ? "Save failed locally"
        : pendingSaveCount.current > 0
          ? "Saving locally…"
          : "Saved locally",
    );
  }

  async function add(formData: FormData) {
    const title = formData.get("title") as string;
    const trimmed = title.trim();
    if (!trimmed) return;
    const generation = ++latestSaveGeneration.current;
    pendingSaveCount.current += 1;
    latestSaveFailed.current = false;
    renderLocalSaveState();
    try {
      const write = db.insert(app.todos, { title: trimmed, done: false });
      await write.wait({ tier: "local" });
    } catch {
      if (generation === latestSaveGeneration.current) latestSaveFailed.current = true;
    } finally {
      pendingSaveCount.current -= 1;
      if (generation === latestSaveGeneration.current || pendingSaveCount.current === 0)
        renderLocalSaveState();
    }
  }

  return (
    <section className="todo-widget">
      <h2>Your todos</h2>
      <form action={add}>
        <input type="text" name="title" placeholder="Add a task" aria-label="New todo" />
        <button type="submit">Add</button>
      </form>
      <p role="status" aria-live="polite">
        {localSaveState}
      </p>
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
