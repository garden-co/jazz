import { createDb, type DbConfig } from "jazz-tools";
import { app } from "../session-app.js";

export async function authSessionExamples(config: DbConfig) {
  const db = await createDb(config);

  // #region auth-session-ts-hook
  const session = db.getAuthState().session;
  // #endregion auth-session-ts-hook

  // #region auth-session-ts-user
  const sessionUser = session?.user ?? null;
  // #endregion auth-session-ts-user

  // #region auth-session-ts-query
  const ownedTodos = sessionUser ? await db.all(app.todos.where({ owner_id: sessionUser })) : [];
  // #endregion auth-session-ts-query

  // #region auth-session-ts-insert
  function addOwnedTodo(title: string) {
    if (!sessionUser) return;

    db.insert(app.todos, {
      title,
      done: false,
      owner_id: sessionUser,
    });
  }
  // #endregion auth-session-ts-insert

  void ownedTodos;
  void addOwnedTodo;

  return db;
}
