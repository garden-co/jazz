import { schema as s } from "jazz-tools";
import { useAll, useDb, useSession } from "jazz-tools/react";

// #region owned-schema
const schema = {
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
// #endregion owned-schema

// #region owned-permissions
s.definePermissions(app, ({ policy, session }) => {
  policy.todos.allowRead.where({ $createdBy: session.user_id });
  policy.todos.allowInsert.always();
  policy.todos.allowUpdate.where({ $createdBy: session.user_id });
  policy.todos.allowDelete.where({ $createdBy: session.user_id });
});
// #endregion owned-permissions

// #region owned-query
export function MyTodos() {
  const todos = useAll(app.todos.where({ done: false }));

  if (!todos) return <p>Loading…</p>;

  return (
    <ul>
      {todos.map((todo) => (
        <li key={todo.id}>{todo.title}</li>
      ))}
    </ul>
  );
}
// #endregion owned-query

// #region owned-insert
export function AddTodo() {
  const db = useDb();

  function handleAdd(title: string) {
    db.insert(app.todos, { title, done: false });
  }

  return <button onClick={() => handleAdd("Buy milk")}>Add</button>;
}
// #endregion owned-insert

// #region owned-schema-explicit
const schemaExplicit = {
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
    owner_id: s.string(),
  }),
};

type ExplicitAppSchema = s.Schema<typeof schemaExplicit>;
export const explicitApp: s.App<ExplicitAppSchema> = s.defineApp(schemaExplicit);
// #endregion owned-schema-explicit

// #region owned-permissions-explicit
s.definePermissions(explicitApp, ({ policy, session }) => {
  policy.todos.allowRead.where({ owner_id: session.user_id });
  policy.todos.allowInsert.where({ owner_id: session.user_id });
  policy.todos.allowUpdate.where({ owner_id: session.user_id });
  policy.todos.allowDelete.where({ owner_id: session.user_id });
});
// #endregion owned-permissions-explicit
