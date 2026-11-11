import { useDb } from "jazz-tools/solid";

export function GetDbExample() {
  const db = useDb();

  // Query hooks return the same handle, so a component that reads and writes needs one hook:
  // const todos = useAll(app.todos);
  // todos.db.insert(app.todos, { title: "Buy milk", done: false });
  void db;
  return null;
}
