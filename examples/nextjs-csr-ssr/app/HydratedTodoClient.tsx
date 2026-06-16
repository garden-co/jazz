"use client";

import { JazzProvider, useAll } from "jazz-tools/react";
import type { DehydratedSnapshot } from "jazz-tools/backend";
import { app } from "../schema";

type Props = { snapshot: DehydratedSnapshot };

export default function HydratedTodoClient({ snapshot }: Props) {
  return (
    <JazzProvider
      config={{
        appId: process.env.NEXT_PUBLIC_JAZZ_APP_ID!,
        serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!,
      }}
      ssr
    >
      <TodoList snapshot={snapshot} />
    </JazzProvider>
  );
}

// The snapshot travels with its query: `useAll` seeds the rows for the first
// render and fills the store from the bundle, so there's no flash when live sync
// connects.
function TodoList({ snapshot }: { snapshot: DehydratedSnapshot }) {
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
