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
      snapshot={snapshot}
    >
      <TodoList snapshot={snapshot} />
    </JazzProvider>
  );
}

// The snapshot is co-located with its query: `useAll` seeds the rows for the
// synchronous first paint and hydrates the store from the bundle, so there's no
// cold-load flash when live sync connects.
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
