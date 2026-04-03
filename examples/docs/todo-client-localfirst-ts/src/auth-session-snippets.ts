import { createDb, resolveClientSession, type DbConfig } from "jazz-tools";
import { app } from "../session-app.js";

export async function authSessionExamples(config: DbConfig) {
  const db = await createDb(config);

  // #region auth-session-ts-hook
  const session = await resolveClientSession(config);
  // #endregion auth-session-ts-hook

  // #region auth-session-ts-user-id
  const sessionUserId = session?.user_id ?? null;
  // #endregion auth-session-ts-user-id

  // #region auth-session-ts-query
  const ownedTodos = sessionUserId
    ? await db.all(app.todos.where({ owner_id: sessionUserId }))
    : [];
  // #endregion auth-session-ts-query

  // #region auth-session-ts-insert
  function addOwnedTodo(title: string) {
    if (!sessionUserId) return;

    db.insert(app.todos, {
      title,
      done: false,
      owner_id: sessionUserId,
    });
  }
  // #endregion auth-session-ts-insert

  void ownedTodos;
  void addOwnedTodo;

  return db;
}
