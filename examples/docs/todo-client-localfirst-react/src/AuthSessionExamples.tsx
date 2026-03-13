// #region auth-session-react
import { useDb, useSession } from "jazz-tools/react";
import { app } from "../schema/session-app.js";

export function AuthSessionExamples() {
  const db = useDb();
  const session = useSession();

  async function loadOwnedTodos() {
    if (!session) return [];
    return db.all(app.todos.where({ owner_id: session.user_id }));
  }

  function addOwnedTodo(title: string) {
    if (!session) return;

    db.insert(app.todos, {
      title,
      done: false,
      owner_id: session.user_id,
    });
  }

  return null;
}
// #endregion auth-session-react
