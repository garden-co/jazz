import { useState } from "react";
import { useDb, useAll, useSession } from "jazz-tools/react";
import { toast } from "sonner";
import { app } from "../schema.js";

export function TodoList() {
  const [filterTitle, setFilterTitle] = useState("");
  const [showDoneOnly, setShowDoneOnly] = useState(false);
  // null = the "main" branch; otherwise the id of a `branches` row.
  const [currentBranchId, setCurrentBranchId] = useState<string | null>(null);
  const [newBranchName, setNewBranchName] = useState("");

  const trimmedFilterTitle = filterTitle.trim();
  let todosQuery = app.todos;
  if (trimmedFilterTitle) {
    todosQuery = todosQuery.where({ title: { contains: trimmedFilterTitle } });
  }
  if (showDoneOnly) {
    todosQuery = todosQuery.where({ done: true });
  }

  const db = useDb();
  const session = useSession();
  const sessionUserId = session?.user_id ?? null;

  // Branches live on main; list the ones this session owns.
  const branches = useAll(app.branches) ?? [];

  // #region reading-reactive-hooks-react
  // Reactive read scoped to the selected branch (or main when undefined).
  const todos = useAll(todosQuery, currentBranchId ? { branch: currentBranchId } : undefined) ?? [];
  // #endregion reading-reactive-hooks-react

  // Writes go through a branch-scoped view when a branch is selected.
  const writer = currentBranchId ? db.branch(currentBranchId) : db;
  const currentBranchName = branches.find((b) => b.id === currentBranchId)?.name ?? null;

  const [title, setTitle] = useState("");

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!title.trim() || !sessionUserId) return;
    writer.insert(app.todos, { title: title.trim(), done: false, owner_id: sessionUserId });
    setTitle("");
  };

  const handleCreateBranch = (e: React.FormEvent) => {
    e.preventDefault();
    if (!newBranchName.trim() || !sessionUserId) return;
    const { value } = db.insert(app.branches, {
      name: newBranchName.trim(),
      owner_id: sessionUserId,
    });
    setNewBranchName("");
    setCurrentBranchId(value.id); // jump onto the freshly created branch
  };

  return (
    <>
      <section
        aria-label="Branch"
        style={{ marginBottom: "1rem", padding: "0.5rem", border: "1px solid #ccc" }}
      >
        <strong>Branch:</strong>{" "}
        <select
          aria-label="Current branch"
          value={currentBranchId ?? "main"}
          onChange={(e) => setCurrentBranchId(e.target.value === "main" ? null : e.target.value)}
        >
          <option value="main">main</option>
          {branches.map((b) => (
            <option key={b.id} value={b.id}>
              {b.name}
            </option>
          ))}
        </select>{" "}
        <span data-testid="current-branch">
          (viewing <em>{currentBranchName ?? "main"}</em>)
        </span>
      </section>

      <form onSubmit={handleSubmit}>
        <input
          type="text"
          value={title}
          onChange={(e) => setTitle(e.target.value)}
          placeholder={
            currentBranchName ? `Add a todo on "${currentBranchName}"` : "What needs to be done?"
          }
          required
        />
        <button type="submit" disabled={!sessionUserId}>
          Add
        </button>
      </form>

      <form onSubmit={handleCreateBranch}>
        <input
          type="text"
          value={newBranchName}
          onChange={(e) => setNewBranchName(e.target.value)}
          placeholder="New branch name"
          aria-label="New branch name"
        />
        <button type="submit" disabled={!sessionUserId}>
          Create branch
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
      </div>
      <ul id="todo-list">
        {todos.map((todo) => (
          <li key={todo.id} className={todo.done ? "done" : ""}>
            <input
              type="checkbox"
              checked={todo.done}
              onChange={() => {
                try {
                  writer.update(app.todos, todo.id, { done: !todo.done });
                } catch {
                  toast.error("You don't have permission to update this task");
                }
              }}
              className="toggle"
            />
            <span>{todo.title}</span>
            {todo.description && <small>{todo.description}</small>}
            <button
              className="delete-btn"
              onClick={() => {
                try {
                  writer.delete(app.todos, todo.id);
                } catch {
                  toast.error("You don't have permission to delete this task");
                }
              }}
            >
              &times;
            </button>
          </li>
        ))}
      </ul>
    </>
  );
}
