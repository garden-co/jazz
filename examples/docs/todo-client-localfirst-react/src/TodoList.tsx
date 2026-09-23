import { useRef, useState } from "react";
// #region reading-reactive-hooks-react
import { useDb, useAll } from "jazz-tools/react";
import { app } from "../schema.js";

const queryTierOptions = [
  { value: "local-first", label: "Local-first" },
  { value: "remote-if-possible", label: "Remote if possible" },
  { value: "remote", label: "Remote only" },
] as const;

type QueryTier = (typeof queryTierOptions)[number]["value"];

const queryOptimismOptions = [
  { value: "show-previews", label: "Show previews" },
  { value: "wait-for-tier", label: "Wait for the selected tier" },
] as const;

type QueryOptimism = (typeof queryOptimismOptions)[number]["value"];

export function TodoList() {
  // #region read-write-react
  const db = useDb();
  // #endregion reading-reactive-hooks-react
  // #region reading-filtering-react
  const { data: incompleteTodos } = useAll(
    app.todos.where({ done: false }).orderBy("title", "asc").limit(50),
  );
  // #endregion reading-filtering-react

  // #region where-subscription-react
  const pending = useAll(app.todos.where({ done: false }));
  // #endregion where-subscription-react

  // #region reading-tier-react
  const todosAtEdgeDurability = useAll(app.todos, { tier: "global" });
  // #endregion reading-tier-react

  // #region reading-loading-state-react
  // Remote-if-possible may show a local preview before authority confirmation.
  const [tier, setTier] = useState<QueryTier>("local-first");
  const [optimism, setOptimism] = useState<QueryOptimism>("show-previews");
  const tierHelpDialog = useRef<HTMLDialogElement>(null);
  const allTodos = useAll(app.todos, { tier });
  const selectedTierLabel = queryTierOptions.find(({ value }) => value === tier)?.label ?? tier;
  const settlementLabel =
    allTodos.highestSettledAt === "unconfirmed"
      ? "Not confirmed yet"
      : allTodos.highestSettledAt === "local"
        ? "On device"
        : "Synced";
  const isShowingPreview =
    optimism === "show-previews" && allTodos.isLoading && allTodos.data !== undefined;
  const waitingForTier = optimism === "wait-for-tier" && allTodos.isLoading;
  const todos = waitingForTier ? [] : (allTodos.data ?? []);
  // `allTodos.data` may contain a local preview while `allTodos.isLoading` is `true`.
  // It is `undefined` only when no result is available yet.
  // Once the requested first result is ready, `allTodos.isLoading` is `false`.
  // A loaded empty query returns `[]`.
  // #endregion reading-loading-state-react

  // #region reading-conditional-query-react
  const [filter, setFilter] = useState<string | null>(null);
  const { data: filtered } = useAll(
    filter ? app.todos.where({ title: { contains: filter } }) : undefined,
  );
  // #endregion reading-conditional-query-react

  // #region writing-use-db-react
  function addTodo(todoTitle: string) {
    db.insert(app.todos, { title: todoTitle, done: false });
  }

  function toggleTodo(todo: { id: string; done: boolean }) {
    db.update(app.todos, todo.id, { done: !todo.done });
  }

  function removeTodo(id: string) {
    db.delete(app.todos, id);
  }
  // #endregion writing-use-db-react
  // #endregion read-write-react

  const [title, setTitle] = useState("");

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!title.trim()) return;
    addTodo(title.trim());
    setTitle("");
  };

  return (
    <>
      <div className="query-controls">
        <label className="query-tier-control" htmlFor="query-tier">
          Query read tier
          <select
            id="query-tier"
            value={tier}
            onChange={(event) => setTier(event.currentTarget.value as QueryTier)}
          >
            {queryTierOptions.map(({ value, label }) => (
              <option key={value} value={value}>
                {label}
              </option>
            ))}
          </select>
        </label>
        <button
          id="tier-help-button"
          type="button"
          aria-haspopup="dialog"
          onClick={() => tierHelpDialog.current?.showModal()}
        >
          Explain choices
        </button>
        <label className="query-tier-control" htmlFor="query-optimism">
          UI optimism
          <select
            id="query-optimism"
            value={optimism}
            onChange={(event) => setOptimism(event.currentTarget.value as QueryOptimism)}
          >
            {queryOptimismOptions.map(({ value, label }) => (
              <option key={value} value={value}>
                {label}
              </option>
            ))}
          </select>
        </label>
        <span
          className={`settlement-badge settlement-badge--${allTodos.highestSettledAt}`}
          role="status"
          aria-label={`Selected read tier: ${selectedTierLabel}. Highest settlement this subscription has observed: ${settlementLabel}.`}
        >
          Selected: {selectedTierLabel} · Highest observed: {settlementLabel}
        </span>
      </div>
      <dialog
        id="tier-help-dialog"
        ref={tierHelpDialog}
        aria-labelledby="tier-help-title"
        aria-describedby="tier-help-intro"
      >
        <h2 id="tier-help-title">About these query choices</h2>
        <p id="tier-help-intro">
          The read tier sets which data this query waits for. UI optimism controls whether this list
          shows available data while it is still loading.
        </p>
        <h3>Read tier</h3>
        <ul>
          <li>
            <strong>Local-first:</strong> Use locally cached data and pending local writes right
            away. The query keeps syncing.
          </li>
          <li>
            <strong>Remote if possible:</strong> When connected, use remote-scope data and eligible
            pending local changes. Local data is used only after an explicit disconnect.
          </li>
          <li>
            <strong>Remote only:</strong> Wait for server-confirmed results. Pending local writes
            are excluded, and the query waits while offline. It does not show an early preview.
          </li>
        </ul>
        <h3>UI optimism</h3>
        <ul>
          <li>
            <strong>Show previews:</strong> Show data already available while the query is still
            loading.
          </li>
          <li>
            <strong>Wait for the selected tier:</strong> Hide available data until the requested
            tier is ready.
          </li>
        </ul>
        <p>
          The badge shows the tier you selected and the highest settlement this subscription has
          observed. It does not mean every displayed row reached that level.
        </p>
        <button id="tier-help-close" type="button" onClick={() => tierHelpDialog.current?.close()}>
          Close
        </button>
      </dialog>
      <form onSubmit={handleSubmit}>
        <input
          type="text"
          value={title}
          onChange={(e) => setTitle(e.target.value)}
          placeholder="What needs to be done?"
          required
        />
        <button type="submit">Add</button>
      </form>
      {allTodos.isLoading && (
        <p id="query-preview-status" aria-live="polite">
          {isShowingPreview
            ? "Showing available data while waiting for the selected tier."
            : waitingForTier
              ? "Waiting for the selected tier before showing this list."
              : "Waiting for data from the selected tier."}
        </p>
      )}
      <ul id="todo-list" aria-busy={allTodos.isLoading}>
        {todos.map((todo) => (
          <li key={todo.id} className={todo.done ? "done" : ""}>
            <input
              type="checkbox"
              checked={todo.done}
              onChange={() => void toggleTodo(todo)}
              className="toggle"
            />
            <span>{todo.title}</span>
            {todo.description && <small>{todo.description}</small>}
            <button className="delete-btn" onClick={() => void removeTodo(todo.id)}>
              &times;
            </button>
          </li>
        ))}
      </ul>
    </>
  );
}
