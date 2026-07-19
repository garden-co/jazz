import { JazzProvider, useAll, useDb, useSession } from "jazz-tools/react";
import { app } from "../schema.js";

function YourApp() {
  return null;
}

// #region provider-react
export function ProviderExample() {
  return (
    <JazzProvider
      config={{
        appId: "my-app",
      }}
      fallback={<p>Loading...</p>}
    >
      <YourApp />
    </JazzProvider>
  );
}
// #endregion provider-react

// #region live-query-react
export function LiveQueryExample() {
  const todos = useAll(app.todos.where({ done: false }));

  // undefined = not yet connected; [] = connected, no rows; [...] = rows present
  if (todos === undefined) return <p>Loading...</p>;

  return (
    <ul>
      {todos.map((todo) => (
        <li key={todo.id}>{todo.title}</li>
      ))}
    </ul>
  );
}
// #endregion live-query-react

// #region db-access-react
export function DbAccessExample() {
  // Must be called at component top level (rules of hooks)
  const db = useDb();

  async function addTodo(title: string) {
    await db.insert(app.todos, { title, done: false });
  }

  void addTodo;
  return null;
}
// #endregion db-access-react

// #region session-react
export function SessionExample() {
  const session = useSession(); // { user_id: string } | null

  void session;
  return null;
}
// #endregion session-react
