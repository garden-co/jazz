import { createMemo } from "solid-js";
import { useAll, useDb, useSession } from "jazz-tools/solid";
import { app } from "../session-app.js";

export function AuthSessionExamples() {
  const db = useDb();
  const session = useSession();
  const sessionUserId = createMemo(() => session()?.user_id ?? null);
  const ownedTodos = useAll(() => ({
    query: sessionUserId() ? app.todos.where({ owner_id: sessionUserId()! }) : undefined,
  }));

  function addOwnedTodo(title: string) {
    const ownerId = sessionUserId();
    if (!ownerId) return;
    db().insert(app.todos, { title, done: false, owner_id: ownerId });
  }

  void ownedTodos;
  void addOwnedTodo;
  return null;
}
