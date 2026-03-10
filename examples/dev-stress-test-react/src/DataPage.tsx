import { useCallback, useEffect, useState } from "react";
import { useAll, useDb, useSession } from "jazz-tools/react";
import { app } from "../schema/app.js";

type OrderColumn = "id" | "title";
type OrderDirection = "asc" | "desc";
type TodoFilters = {
  filterTitle: string;
  showDoneOnly: boolean;
  projectJoin: boolean;
};

function readFiltersFromLocation(): TodoFilters {
  const params = new URLSearchParams(window.location.search);

  const filterTitle = params.get("title") ?? "";
  const doneParam = params.get("done");
  const joinParam = params.get("join");

  return {
    filterTitle,
    showDoneOnly: doneParam === null ? true : doneParam.toLowerCase() === "true",
    projectJoin: joinParam !== null && joinParam.toLowerCase() === "true",
  };
}

function syncLocationSearch({ filterTitle, showDoneOnly, projectJoin }: TodoFilters): void {
  const params = new URLSearchParams(window.location.search);
  const trimmedFilterTitle = filterTitle.trim();

  if (trimmedFilterTitle) {
    params.set("title", filterTitle);
  } else {
    params.delete("title");
  }
  params.set("done", showDoneOnly ? "true" : "false");
  params.set("join", projectJoin ? "true" : "false");

  const nextSearch = params.toString();
  const nextPath = `${window.location.pathname}${nextSearch ? `?${nextSearch}` : ""}${window.location.hash}`;
  const currentPath = `${window.location.pathname}${window.location.search}${window.location.hash}`;

  if (currentPath !== nextPath) {
    window.history.replaceState({}, "", nextPath);
  }
}

function OrderingControls({
  column,
  direction,
  onColumnChange,
  onDirectionChange,
}: {
  column: OrderColumn;
  direction: OrderDirection;
  onColumnChange: (column: OrderColumn) => void;
  onDirectionChange: (direction: OrderDirection) => void;
}) {
  return (
    <div>
      <label>
        Order by
        <select
          value={column}
          onChange={(event) => onColumnChange(event.target.value as OrderColumn)}
        >
          <option value="id">id</option>
          <option value="title">title</option>
        </select>
      </label>
      <label>
        Direction
        <select
          value={direction}
          onChange={(event) => onDirectionChange(event.target.value as OrderDirection)}
        >
          <option value="asc">asc</option>
          <option value="desc">desc</option>
        </select>
      </label>
    </div>
  );
}

export function DataPage() {
  const initialFilters = readFiltersFromLocation();

  const [filterTitle, setFilterTitle] = useState(initialFilters.filterTitle);
  const [showDoneOnly, setShowDoneOnly] = useState(initialFilters.showDoneOnly);
  const [projectJoin, setProjectJoin] = useState(initialFilters.projectJoin);
  const [orderColumn, setOrderColumn] = useState<OrderColumn>("id");
  const [orderDirection, setOrderDirection] = useState<OrderDirection>("asc");
  const trimmedFilterTitle = filterTitle.trim();
  let todosQuery = app.todos;
  if (trimmedFilterTitle) {
    todosQuery = todosQuery.where({ title: { contains: trimmedFilterTitle } });
  }
  if (showDoneOnly) {
    todosQuery = todosQuery.where({ done: true });
  }
  todosQuery = todosQuery.orderBy(orderColumn, orderDirection).limit(50);

  const db = useDb();
  // #region reading-reactive-hooks-react
  const todos = useAll(todosQuery) ?? [];
  // #endregion reading-reactive-hooks-react
  const session = useSession();
  const sessionUserId = session?.user_id ?? null;
  const [title, setTitle] = useState("");

  let projectJoinQuery = app.projects.limit(20);

  if (projectJoin) {
    projectJoinQuery = projectJoinQuery.include({
      todosViaProject: true,
    });
  }

  const projectRows = useAll(projectJoinQuery) ?? [];

  const syncFromLocation = useCallback(() => {
    const {
      filterTitle: nextFilterTitle,
      showDoneOnly: nextShowDoneOnly,
      projectJoin: nextProjectJoin,
    } = readFiltersFromLocation();
    setFilterTitle((currentFilterTitle) =>
      currentFilterTitle === nextFilterTitle ? currentFilterTitle : nextFilterTitle,
    );
    setShowDoneOnly((currentShowDoneOnly) =>
      currentShowDoneOnly === nextShowDoneOnly ? currentShowDoneOnly : nextShowDoneOnly,
    );
    setProjectJoin((currentProjectJoin) =>
      currentProjectJoin === nextProjectJoin ? currentProjectJoin : nextProjectJoin,
    );
  }, []);

  useEffect(() => {
    const syncFromUrl = () => {
      syncFromLocation();
    };
    window.addEventListener("popstate", syncFromUrl);
    window.addEventListener("hashchange", syncFromUrl);
    return () => {
      window.removeEventListener("popstate", syncFromUrl);
      window.removeEventListener("hashchange", syncFromUrl);
    };
  }, [syncFromLocation]);

  useEffect(() => {
    syncLocationSearch({ filterTitle, showDoneOnly, projectJoin });
  }, [filterTitle, showDoneOnly, projectJoin]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!title.trim() || !sessionUserId) return;
    db.insert(app.todos, { title: title.trim(), done: false, owner_id: sessionUserId });
    setTitle("");
  };

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
          onChange={(e) => setFilterTitle(e.target.value)}
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
        <label>
          <input
            type="checkbox"
            checked={projectJoin}
            onChange={(e) => setProjectJoin(e.target.checked)}
          />
          Project join
        </label>
      </div>
      <OrderingControls
        column={orderColumn}
        direction={orderDirection}
        onColumnChange={setOrderColumn}
        onDirectionChange={setOrderDirection}
      />
      <div>
        <h2>Project join preview</h2>
        <ul>
          {projectRows.map((project) => (
            <li key={project.id}>
              {project.name}: {project.todosViaProject?.map((todo) => todo.title).join(", ")}
            </li>
          ))}
        </ul>
      </div>
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
            <button className="delete-btn" onClick={() => db.delete(app.todos, todo.id)}>
              &times;
            </button>
          </li>
        ))}
      </ul>
    </>
  );
}
