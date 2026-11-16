import { useAll, useJazzSession } from "jazz-tools/react";
import { app } from "../session-app/jazz.generated.js";

export function AuthSessionExamples() {
  // #region auth-session-react-hook
  const { user } = useJazzSession();
  // #endregion auth-session-react-hook

  // #region auth-session-react-user
  const sessionUser = user?.accountId ?? null;
  // #endregion auth-session-react-user

  // #region auth-session-react-query
  const { data: ownedTodos = [], db } = useAll(
    sessionUser ? app.todos.where({ owner_id: sessionUser }) : undefined,
  );
  // #endregion auth-session-react-query

  // #region auth-session-react-insert
  function addOwnedTodo(title: string) {
    if (!sessionUser) return;

    db.insert(app.todos, {
      title,
      done: false,
      owner_id: sessionUser,
    });
  }
  // #endregion auth-session-react-insert

  void ownedTodos;
  void addOwnedTodo;

  return null;
}
