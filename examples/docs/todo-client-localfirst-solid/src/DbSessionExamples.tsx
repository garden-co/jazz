import { useDb, useJazzSession } from "jazz-tools/solid";
import { app } from "../jazz.generated.js";

export function DbSessionExamples() {
  const db = useDb();
  // Who is signed in: { accountId, kind: "device" | "backend", identity, claims } | null.
  // Your permissions see the same user as `session.user` (the accountId),
  // `session.identity` and `session.claims`.
  const { snapshot } = useJazzSession(); // snapshot().user

  async function addTodo(title: string) {
    await db().insert(app.todos, { title, done: false });
  }

  void addTodo;
  void snapshot;
  return null;
}
