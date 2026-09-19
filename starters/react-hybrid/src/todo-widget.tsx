import { useEffect, useRef, useState } from "react";
import { useDb, useAll } from "jazz-tools/react";
import { app } from "../schema";

type DeleteOperation = { lifecycle: number };

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
  const deleteFailures = useRef(new Map<string, string>());
  const [mutationError, setMutationError] = useState<string | null>(null);

  function renderDeleteStatus() {
    const failure = deleteFailures.current.values().next().value as string | undefined;
    setDeleteStatus(failure ?? (pendingDeletes.current.size > 0 ? "Deleting…" : null));
  }

  useEffect(() => {
    const lifecycle = ++deleteLifecycle.current;
    pendingDeletes.current.clear();
    deleteFailures.current.clear();
    setDeleteStatus(null);
    setMutationError(null);
    // Local durability does not imply server acceptance. Jazz restores rejected
    // writes and reports later authority failures here, including adds/updates.
    const unsubscribe = db.onMutationError((event) => {
      if (deleteLifecycle.current !== lifecycle) return;
      setMutationError(`A change was rejected: ${event.reason}`);
    });
    return () => {
      unsubscribe();
      if (deleteLifecycle.current === lifecycle) {
        deleteLifecycle.current += 1;
      }
      pendingDeletes.current.clear();
      deleteFailures.current.clear();
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
    const operation: DeleteOperation = { lifecycle: deleteLifecycle.current };
    // Register before starting the write so an older completion cannot settle a retry.
    pendingDeletes.current.set(todoId, operation);
    deleteFailures.current.delete(todoId);
    renderDeleteStatus();
    try {
      const write = db.delete(app.todos, todoId);
      // The starter reports local persistence; remote sync proceeds independently.
      await write.wait({ tier: "local" });
    } catch {
      if (
        deleteLifecycle.current === operation.lifecycle &&
        pendingDeletes.current.get(todoId) === operation
      ) {
        deleteFailures.current.set(todoId, `Delete failed: ${title}`);
      }
    } finally {
      if (
        deleteLifecycle.current === operation.lifecycle &&
        pendingDeletes.current.get(todoId) === operation
      ) {
        pendingDeletes.current.delete(todoId);
        renderDeleteStatus();
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
        {mutationError ?? deleteStatus ?? localSaveState}
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
