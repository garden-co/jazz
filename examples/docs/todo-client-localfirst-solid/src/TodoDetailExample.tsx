import { Match, Switch } from "solid-js";
import { useOne } from "jazz-tools/solid";
import { app } from "../schema.js";

// #region reading-one-solid
export function TodoDetailExample(props: { id: string }) {
  const todo = useOne(() => ({ query: app.todos.where({ id: props.id }) }));
  // todo.data: undefined = loading; null = no row matches or you can't read it

  return (
    <Switch>
      <Match when={todo.data === undefined}>
        <p>Loading…</p>
      </Match>
      <Match when={todo.data === null}>
        <p>Todo not found.</p>
      </Match>
      <Match when={todo.data}>{(found) => <h1>{found().title}</h1>}</Match>
    </Switch>
  );
}
// #endregion reading-one-solid
