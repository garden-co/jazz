import { useAll, useDb, useSession } from "jazz-tools/react";
import { app } from "../session-app.js";

export function AuthSessionExamples() {
  const db = useDb();

  // #region auth-session-react-hook
  const session = useSession();
  // #endregion auth-session-react-hook

  // #region auth-session-react-user-id
  const sessionUserId = session?.user_id ?? null;
  // #endregion auth-session-react-user-id

  // #region auth-session-react-query
  const ownedTodos =
    useAll(sessionUserId ? app.todos.where({ owner_id: sessionUserId }) : undefined) ?? [];
  // #endregion auth-session-react-query

  // #region auth-session-react-insert
  function addOwnedTodo(title: string) {
    if (!sessionUserId) return;

    db.insert(app.todos, {
      title,
      done: false,
      owner_id: sessionUserId,
    });
  }
  // #endregion auth-session-react-insert

  void ownedTodos;
  void addOwnedTodo;

  return null;
}
