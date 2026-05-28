"use client";

import { useEffect, useState } from "react";
import { BrowserAuthSecretStore } from "jazz-tools";
import { JazzProvider, useAll } from "jazz-tools/react";
import type { DehydratedSnapshot } from "jazz-tools/backend";
import { app } from "../schema";

type Props = { snapshot: DehydratedSnapshot };

// Wraps a client tree with JazzProvider seeded from a server-rendered
// snapshot.
export default function HydratedTodoClient({ snapshot }: Props) {
  const [secret, setSecret] = useState("");
  const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID!;

  useEffect(() => {
    BrowserAuthSecretStore.getOrCreateSecret({ appId }).then(setSecret);
  }, [appId]);

  if (!secret) return null;

  return (
    <JazzProvider
      config={{
        appId,
        serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!,
        secret,
      }}
      snapshot={snapshot}
    >
      <TodoList />
    </JazzProvider>
  );
}

function TodoList() {
  // Because the snapshot pre-fills the orchestrator's cache for this query,
  // `useAll` returns the seeded data synchronously on first render — no
  // undefined/loading state on hydration.
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
