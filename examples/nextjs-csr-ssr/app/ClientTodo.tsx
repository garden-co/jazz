"use client";

import { useState, useEffect } from "react";
import { app } from "../schema";
import { createAccountManager, type AccountHandle } from "jazz-tools";
import { JazzProvider, useAll, useDb } from "jazz-tools/react";

export default function ClientTodo() {
  const [account, setAccount] = useState<AccountHandle>();
  const [error, setError] = useState<string>();
  const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID!;

  useEffect(() => {
    let cancelled = false;
    void createAccountManager({
      appId,
      serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!,
    })
      .then((accounts) => {
        if (!cancelled) setAccount(accounts.getLoggedIn() ?? accounts.createLocalFirst());
      })
      .catch((error: unknown) => {
        if (!cancelled) setError(error instanceof Error ? error.message : String(error));
      });
    return () => {
      cancelled = true;
    };
  }, [appId]);

  if (error) return <p role="alert">{error}</p>;
  if (!account) return <p>Loading account…</p>;

  return (
    <JazzProvider
      config={{
        appId,
        serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!,
        account,
      }}
    >
      <TodoForm />
      <TodoList />
    </JazzProvider>
  );
}

function TodoList() {
  const { data: todos = [] } = useAll(app.todos);
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
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [saveError, setSaveError] = useState<string>();
  const handleSubmit = async (e: React.SubmitEvent<HTMLFormElement>) => {
    e.preventDefault();
    const form = e.target as HTMLFormElement;
    const title = form.titleField.value.trim();
    if (!title || saving) return;
    setSaving(true);
    setSaved(false);
    setSaveError(undefined);
    try {
      // The row appears optimistically; acknowledge saving only after the
      // browser worker has persisted it so it can survive a reload.
      await db.insert(app.todos, { title, done: false }).wait({ tier: "local" });
      form.reset();
      setSaved(true);
    } catch (error: unknown) {
      setSaveError(error instanceof Error ? error.message : String(error));
    } finally {
      setSaving(false);
    }
  };
  return (
    <>
      <form onSubmit={handleSubmit} className="flex gap-2">
        <input
          disabled={saving}
          name="titleField"
          type="text"
          placeholder="New todo…"
          className="flex-1 text-sm bg-transparent border border-foreground/15 rounded px-3 py-1.5 outline-none focus:border-foreground/40 placeholder:text-foreground/25"
        />
        <button
          disabled={saving}
          type="submit"
          className="text-sm px-3 py-1.5 border border-foreground/15 rounded hover:bg-foreground/5 transition-colors cursor-pointer"
        >
          Add
        </button>
      </form>
      <p role="status">{saving ? "Saving…" : saved ? "Saved locally" : ""}</p>
      {saveError && <p role="alert">{saveError}</p>}
    </>
  );
}
