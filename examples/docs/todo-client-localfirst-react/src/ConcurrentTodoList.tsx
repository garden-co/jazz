import { Suspense, useDeferredValue, useState, useTransition } from "react";
import { useAllSuspense, useDb } from "jazz-tools/react";
import { app, type TodoQueryBuilder } from "../schema.js";

// #region reading-concurrent-rendering-react
export function ConcurrentTodoList() {
  const db = useDb();
  const [title, setTitle] = useState("");
  const [filterTitle, setFilterTitle] = useState("");
  const [showDoneOnly, setShowDoneOnly] = useState(false);
  const [page, setPage] = useState(0);
  const [isPending, startTransition] = useTransition();
  const deferredFilterTitle = useDeferredValue(filterTitle);

  let query = app.todos
    .orderBy("id", "desc")
    .limit(25)
    .offset(page * 25);

  if (deferredFilterTitle.trim()) {
    query = query.where({ title: { contains: deferredFilterTitle.trim() } });
  }
  if (showDoneOnly) {
    query = query.where({ done: true });
  }

  const isLoading = isPending || deferredFilterTitle !== filterTitle;

  function updatePage(nextPage: number) {
    startTransition(() => {
      setPage(nextPage);
    });
  }

  function handleFilterChange(e: React.ChangeEvent<HTMLInputElement>) {
    setFilterTitle(e.target.value);
    startTransition(() => {
      setPage(0);
    });
  }

  async function handleSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const trimmedTitle = title.trim();

    if (!trimmedTitle) {
      return;
    }

    await db.insert(app.todos, { title: trimmedTitle, done: false });
    setTitle("");
  }

  return (
    <>
      <form onSubmit={(e) => void handleSubmit(e)}>
        <input
          type="text"
          value={title}
          onChange={(e) => setTitle(e.target.value)}
          placeholder="What needs to be done?"
          required
        />
        <button type="submit">Add</button>
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

      <Suspense fallback={<p>Loading todos...</p>}>
        <div style={{ opacity: isLoading ? 0.5 : 1, transition: "opacity 0.2s" }}>
          <ConcurrentTodoResults query={query} page={page} onPageChange={updatePage} />
        </div>
      </Suspense>
    </>
  );
}
// #endregion reading-concurrent-rendering-react

function ConcurrentTodoResults({
  query,
  page,
  onPageChange,
}: {
  query: TodoQueryBuilder;
  page: number;
  onPageChange: (nextPage: number) => void;
}) {
  const db = useDb();

  // #region reading-reactive-hooks-suspense-react
  const todos = useAllSuspense(query);
  // #endregion reading-reactive-hooks-suspense-react

  return (
    <>
      <ul id="concurrent-todo-list">
        {todos.map((todo) => (
          <li key={todo.id} className={todo.done ? "done" : ""}>
            <input
              type="checkbox"
              checked={todo.done}
              onChange={() => void db.update(app.todos, todo.id, { done: !todo.done })}
              className="toggle"
            />
            <span>{todo.title}</span>
            <button className="delete-btn" onClick={() => db.delete(app.todos, todo.id)}>
              &times;
            </button>
          </li>
        ))}
      </ul>

      <div>
        Page {page + 1}
        {page > 0 ? <button onClick={() => onPageChange(page - 1)}>Previous</button> : null}
        <button onClick={() => onPageChange(page + 1)}>Next</button>
      </div>
    </>
  );
}
