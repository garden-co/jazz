import { useDb, useSession } from "jazz-tools/solid";
import { app } from "../schema.js";

export function DbSessionExamples() {
  const db = useDb();
  const session = useSession();

  async function addTodo(title: string) {
    await db().insert(app.todos, { title, done: false });
  }

  void addTodo;
  void session;
  return null;
}
