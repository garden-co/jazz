import type { JazzClient, JazzSession } from "jazz-tools/client";
import { app } from "../session-app.js";

export async function authSessionExamples(session: JazzSession<JazzClient>) {
  const db = session.db;

  // #region auth-session-ts-hook
  const user = session.getSnapshot().user;
  // #endregion auth-session-ts-hook

  // #region auth-session-ts-user
  const accountId = user?.accountId ?? null;
  // #endregion auth-session-ts-user

  // #region auth-session-ts-query
  const ownedTodos = accountId ? await db.all(app.todos.where({ owner_id: accountId })) : [];
  // #endregion auth-session-ts-query

  // #region auth-session-ts-insert
  function addOwnedTodo(title: string) {
    if (!accountId) return;

    db.insert(app.todos, {
      title,
      done: false,
      owner_id: accountId,
    });
  }
  // #endregion auth-session-ts-insert

  void ownedTodos;
  void addOwnedTodo;

  return db;
}
