"use client";

import { useState, useEffect } from "react";
import { BrowserAuthSecretStore } from "jazz-tools";
import { JazzProvider } from "jazz-tools/react";
import type { DehydratedSnapshot } from "jazz-tools/backend";
import TodoForm from "./TodoForm";
import TodoList from "./TodoList";

// Pass a snapshot to seed the first render (the prefetch column does); leave it
// off and the panel just waits for the live client (the client-only column). The
// panel cares about nothing else.
export default function TodoPanel({ snapshot }: { snapshot?: DehydratedSnapshot }) {
  const [secret, setSecret] = useState("");
  const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID!;

  useEffect(() => {
    BrowserAuthSecretStore.getOrCreateSecret({ appId }).then(setSecret);
  }, [appId]);

  // No snapshot: wait for the persisted secret before connecting. With one: seed
  // the rows right away (ssr) and let the client connect on its own once it lands.
  if (!snapshot && !secret) return null;

  return (
    <JazzProvider
      config={{
        appId,
        serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!,
        // The client-only column reuses its persisted secret; the seeded column
        // lets the client connect on its own — its rows are already on screen.
        ...(!snapshot && secret ? { secret } : {}),
      }}
      ssr={!!snapshot}
    >
      <TodoForm />
      <TodoList snapshot={snapshot} />
    </JazzProvider>
  );
}
