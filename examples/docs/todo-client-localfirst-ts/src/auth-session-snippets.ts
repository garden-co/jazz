// #region auth-session-ts
import { createDb, resolveClientSession, type DbConfig } from "jazz-tools";
import { app } from "../schema/session-app.js";

export async function readAndWriteOwnedTodos(config: DbConfig) {
  const db = await createDb(config);
  const session = await resolveClientSession(config);

  if (!session) {
    return [];
  }

  db.insert(app.todos, {
    title: "Write docs",
    done: false,
    owner_id: session.user_id,
  });

  return db.all(app.todos.where({ owner_id: session.user_id }));
}
// #endregion auth-session-ts
