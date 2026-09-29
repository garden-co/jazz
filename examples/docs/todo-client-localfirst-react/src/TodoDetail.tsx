import { Suspense } from "react";
import { useOne, useOneSuspense } from "jazz-tools/react";
import { app } from "../schema.js";

// #region reading-one-react
export function TodoDetail({ id }: { id: string }) {
  const { data: todo, error } = useOne(app.todos.where({ id }).include({ project: true }));
  // `todo` is `undefined` while loading, `null` if no row matches or you can't read it

  if (error) return <p>Couldn't load this todo.</p>;
  if (todo === undefined) return <p>Loading…</p>;
  if (todo === null) return <p>Todo not found.</p>;

  return (
    <article>
      <h1>{todo.title}</h1>
      {todo.project && <p>In {todo.project.name}</p>}
    </article>
  );
}
// #endregion reading-one-react

// #region reading-one-suspense-react
export function TodoDetailPage({ id }: { id: string }) {
  return (
    <Suspense fallback={<p>Loading…</p>}>
      <TodoTitle id={id} />
    </Suspense>
  );
}

function TodoTitle({ id }: { id: string }) {
  const todo = useOneSuspense(app.todos.where({ id }));
  return <h1>{todo ? todo.title : "Todo not found"}</h1>;
}
// #endregion reading-one-suspense-react
