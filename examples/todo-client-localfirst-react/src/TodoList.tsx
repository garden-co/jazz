import { Suspense, useDeferredValue, useState, useTransition } from "react";
import { useDb, useAllSuspense, useSession } from "jazz-tools/react";
import { app, type TodoQueryBuilder } from "../schema/app.js";

export function TodoList() {
  const [filterTitle, setFilterTitle] = useState("");
  const [showDoneOnly, setShowDoneOnly] = useState(false);
  const [page, setPage] = useState(0);

  const [isPending, startTransition] = useTransition();

  const deferredFilterTitle = useDeferredValue(filterTitle);

  let todosQuery = app.todos;
  if (deferredFilterTitle) {
    todosQuery = todosQuery.where({ title: { contains: deferredFilterTitle.trim() } });
  }
  if (showDoneOnly) {
    todosQuery = todosQuery.where({ done: true });
  }
  const query = todosQuery
    .orderBy("id", "desc")
    .limit(50)
    .offset(page * 50);

  const db = useDb();
  const session = useSession();
  const sessionUserId = session?.user_id ?? null;
  const [title, setTitle] = useState("");

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!title.trim() || !sessionUserId) return;

    db.insert(app.todos, { title: title.trim(), done: false, owner_id: sessionUserId });
    setTitle("");
  };

  const handlePageChange = (p: number) => {
    startTransition(() => {
      setPage(p);
    });
  };

  const handleFilterChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setFilterTitle(e.target.value);
    setPage(0);
  };

  const isLoading = isPending || deferredFilterTitle !== filterTitle;

  return (
    <>
      <form onSubmit={handleSubmit}>
        <input
          type="text"
          value={title}
          onChange={(e) => setTitle(e.target.value)}
          placeholder="What needs to be done?"
          required
        />
        <button type="submit" disabled={!sessionUserId}>
          Add
        </button>
      </form>
      <div>
        <input
          type="text"
          value={filterTitle}
          onChange={handleFilterChange}
          placeholder="Filter by title (contains)"
          aria-label="Filter by title"
        />
        <label>
          <input
            type="checkbox"
            checked={showDoneOnly}
            onChange={(e) => setShowDoneOnly(e.target.checked)}
          />
          Done only
        </label>
      </div>
      <Suspense fallback={<p>Loading todos…</p>}>
        <div style={{ opacity: isLoading ? 0.5 : 1, transition: "opacity 0.2s" }}>
          <TodoResults query={query} page={page} setPage={handlePageChange} />
        </div>
      </Suspense>
    </>
  );
}

function TodoResults({
  query,
  page,
  setPage,
}: {
  query: TodoQueryBuilder;
  page: number;
  setPage: (p: number) => void;
}) {
  const db = useDb();

  // #region reading-reactive-hooks-react
  const todos = useAllSuspense(query);
  // #endregion reading-reactive-hooks-react

  return (
    <>
      <ul id="todo-list">
        {todos.map((todo) => (
          <li key={todo.id} className={todo.done ? "done" : ""}>
            <input
              type="checkbox"
              checked={todo.done}
              onChange={() => db.update(app.todos, todo.id, { done: !todo.done })}
              className="toggle"
            />
            <span>{todo.title}</span>
            {todo.description && <small>{todo.description}</small>}
            <button className="delete-btn" onClick={() => db.deleteFrom(app.todos, todo.id)}>
              &times;
            </button>
          </li>
        ))}
      </ul>
      Page {page + 1} {page > 0 && <button onClick={() => setPage(page - 1)}>Previous</button>}{" "}
      <button onClick={() => setPage(page + 1)}>Next</button>
    </>
  );
}
