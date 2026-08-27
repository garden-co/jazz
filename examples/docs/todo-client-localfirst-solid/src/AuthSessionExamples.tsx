import { createMemo } from "solid-js";
import { useAll, useDb, useSession } from "jazz-tools/solid";
import { app } from "../session-app.js";

export function AuthSessionExamples() {
  const db = useDb();
  const session = useSession();
  const sessionUser = createMemo(() => session()?.user ?? null);
  const ownedTodos = useAll(() => ({
    query: sessionUser() ? app.todos.where({ owner_id: sessionUser()! }) : undefined,
  }));

  function addOwnedTodo(title: string) {
    const ownerId = sessionUser();
    if (!ownerId) return;
    db().insert(app.todos, { title, done: false, owner_id: ownerId });
  }

  void ownedTodos;
  void addOwnedTodo;
  return null;
}
