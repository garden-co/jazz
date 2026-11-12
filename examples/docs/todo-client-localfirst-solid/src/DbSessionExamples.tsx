import { useDb, useJazzSession, useSession } from "jazz-tools/solid";
import { app } from "../schema.js";

export function DbSessionExamples() {
  const db = useDb();
  // Who is signed in: { accountId, kind: "device" | "backend", identity, claims } | null
  const { snapshot } = useJazzSession(); // snapshot().user
  // What your permissions see as `session` (its `user` is the account id).
  const session = useSession();

  async function addTodo(title: string) {
    await db().insert(app.todos, { title, done: false });
  }

  void addTodo;
  void snapshot;
  void session;
  return null;
}
