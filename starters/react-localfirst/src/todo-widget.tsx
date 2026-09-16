import { useEffect, useRef, useState } from "react";
import { useDb, useAll } from "jazz-tools/react";
import type { WriteHandle } from "jazz-tools";
import { app } from "../schema";

type DeleteWriteHandle = WriteHandle;
type DeleteOperation = {
  todoId: string;
  title: string;
  lifecycle: number;
  write: DeleteWriteHandle;
};
export function TodoWidget() {
  const db = useDb();
  const { data: todos = [] } = useAll(app.todos);
  const [localSaveState, setLocalSaveState] = useState("Ready to save locally");
  const [deleteStatus, setDeleteStatus] = useState<string | null>(null);
  const latestSaveGeneration = useRef(0);
  const pendingSaveCount = useRef(0);
  const latestSaveFailed = useRef(false);
  const deleteLifecycle = useRef(0);
  const pendingDeletes = useRef(new Map<string, DeleteOperation>());

  useEffect(() => {
    const lifecycle = ++deleteLifecycle.current;
    pendingDeletes.current.clear();
    setDeleteStatus(null);
    return () => {
      if (deleteLifecycle.current === lifecycle) {
        deleteLifecycle.current += 1;
      }
      pendingDeletes.current.clear();
    };
  }, [db]);

  function renderLocalSaveState() {
    setLocalSaveState(
      latestSaveFailed.current
        ? "Save failed locally"
        : pendingSaveCount.current > 0
          ? "Saving locally…"
          : "Saved locally",
    );
  }

  async function add(formData: FormData) {
    const title = formData.get("title") as string;
    const trimmed = title.trim();
    if (!trimmed) return;
    const generation = ++latestSaveGeneration.current;
    pendingSaveCount.current += 1;
    latestSaveFailed.current = false;
    renderLocalSaveState();
    try {
      const write = db.insert(app.todos, { title: trimmed, done: false });
      await write.wait({ tier: "local" });
    } catch {
      if (generation === latestSaveGeneration.current) latestSaveFailed.current = true;
    } finally {
      pendingSaveCount.current -= 1;
      if (generation === latestSaveGeneration.current || pendingSaveCount.current === 0) {
        renderLocalSaveState();
      }
    }
  }

  async function remove(todoId: string, title: string) {
    const lifecycle = deleteLifecycle.current;
    setDeleteStatus("Deleting…");

    let write: DeleteWriteHandle;
    try {
      write = db.delete(app.todos, todoId);
    } catch {
      if (deleteLifecycle.current === lifecycle) setDeleteStatus("Delete failed");
      return;
    }

    const operation: DeleteOperation = { todoId, title, lifecycle, write };
    pendingDeletes.current.set(todoId, operation);
    let failed = false;
    try {
      await write.wait({ tier: "local" });
      await write.wait({ tier: "edge" });
    } catch {
      failed = true;
      if (
        deleteLifecycle.current === operation.lifecycle &&
        pendingDeletes.current.get(operation.todoId) === operation
      ) {
        setDeleteStatus(`Delete failed: ${operation.title}`);
      }
    } finally {
      if (
        deleteLifecycle.current === operation.lifecycle &&
        pendingDeletes.current.get(operation.todoId) === operation
      ) {
        pendingDeletes.current.delete(operation.todoId);
        if (!failed && pendingDeletes.current.size === 0) {
          setDeleteStatus(null);
        }
      }
    }
  }

  return (
    <section className="todo-widget">
      <h2>Your todos</h2>
      <form action={add}>
        <input type="text" name="title" placeholder="Add a task" aria-label="New todo" />
        <button type="submit">Add</button>
      </form>
      <p role="status" aria-live="polite">
        {deleteStatus ?? localSaveState}
      </p>
      <ul>
        {todos.map((t) => (
          <li key={t.id} className={t.done ? "done" : ""}>
            <label>
              <input
                type="checkbox"
                checked={t.done}
                onChange={() => db.update(app.todos, t.id, { done: !t.done })}
              />
              <span>{t.title}</span>
            </label>
            <button type="button" aria-label="Delete" onClick={() => void remove(t.id, t.title)}>
              ×
            </button>
          </li>
        ))}
      </ul>
    </section>
  );
}
