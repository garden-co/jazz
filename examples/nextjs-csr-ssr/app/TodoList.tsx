"use client";

import { useAll } from "jazz-tools/react";
import type { DehydratedSnapshot } from "jazz-tools/backend";
import { app } from "../schema";

// Set only in the prefetch column; the client-only column omits it. The snapshot
// seeds the rows for the first render and fills the store from its bundle, so the
// hand-off to live sync is flash-free.
export default function TodoList({ snapshot }: { snapshot?: DehydratedSnapshot }) {
  const todos = useAll(app.todos, { snapshot }) ?? [];
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
