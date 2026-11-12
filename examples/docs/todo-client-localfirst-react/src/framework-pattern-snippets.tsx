import {
  JazzProvider,
  useAll,
  useDb,
  useJazzSession,
  useSession,
  type JazzClientConfig,
} from "jazz-tools/react";
import { app } from "../schema.js";

function YourApp() {
  return null;
}

// #region provider-react
export function ProviderExample({ config }: { config: JazzClientConfig }) {
  return (
    <JazzProvider config={config} loading={<p>Loading...</p>}>
      <YourApp />
    </JazzProvider>
  );
}
// #endregion provider-react

// #region live-query-react
export function LiveQueryExample() {
  const { data: todos, isLoading, error } = useAll(app.todos.where({ done: false }));

  if (isLoading) return <p>Loading...</p>;
  if (error) return <p>Something went wrong!</p>;

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
  // Who is signed in: { accountId, kind: "device" | "backend", identity, claims } | null
  const { user } = useJazzSession();
  // What your permissions see as `session` (its `user` is the account id).
  const session = useSession();

  void user;
  void session;
  return null;
}
// #endregion session-react
