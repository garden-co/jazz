import type { QueryBuilder, QueryOptions } from "jazz-tools/client";
import { app } from "../schema.js";

type Todo = { id: string; title: string; done: boolean };
type MaybePromise<T> = T | Promise<T>;
type MutationResult<T> = {
  value: T;
  wait(options: { tier: "local" | "edge" | "global" }): Promise<T>;
};

export interface TodoDb {
  insert<T, Init>(table: unknown, data: Init): MaybePromise<MutationResult<T>>;
  update(table: unknown, id: string, data: Partial<unknown>): MaybePromise<MutationResult<void>>;
  delete(table: unknown, id: string): MaybePromise<MutationResult<void>>;
  subscribe<T extends { id: string }>(
    query: QueryBuilder<T>,
    callback: (rows: T[]) => void,
    options?: QueryOptions,
  ): () => void;
}

function renderRow(todo: Todo): HTMLLIElement {
  const li = document.createElement("li");
  if (todo.done) li.classList.add("done");
  li.dataset.id = todo.id;

  const label = document.createElement("label");
  const checkbox = document.createElement("input");
  checkbox.type = "checkbox";
  checkbox.checked = todo.done;
  checkbox.dataset.action = "toggle";
  const text = document.createElement("span");
  text.textContent = todo.title;
  label.append(checkbox, text);

  const del = document.createElement("button");
  del.type = "button";
  del.setAttribute("aria-label", "Delete");
  del.dataset.action = "delete";
  del.textContent = "×";

  li.append(label, del);
  return li;
}

export function mountTodoWidget(parent: HTMLElement, db: TodoDb): () => void {
  parent.innerHTML = `
    <section class="todo-widget">
      <h2>Your todos</h2>
      <form>
        <input type="text" name="title" placeholder="Add a task" aria-label="New todo" />
        <button type="submit">Add</button>
      </form>
      <p role="status" aria-live="polite">Ready to save locally</p>
      <ul></ul>
    </section>
  `;
  const form = parent.querySelector<HTMLFormElement>("form")!;
  const input = form.querySelector<HTMLInputElement>("input[name='title']")!;
  const list = parent.querySelector<HTMLUListElement>("ul")!;
  const localSaveStatus = parent.querySelector<HTMLElement>("[role='status']")!;
  let latestSaveGeneration = 0;
  let pendingLocalSaveCount = 0;
  let latestLocalSaveState: "saving" | "saved" | "failed" | "sync-failed" = "saved";

  function renderLocalSaveState() {
    localSaveStatus.textContent =
      latestLocalSaveState === "failed"
        ? "Save failed locally"
        : latestLocalSaveState === "sync-failed"
          ? "Saved locally; sync failed"
          : pendingLocalSaveCount > 0
            ? "Saving locally…"
            : "Saved locally";
  }

  function renderTodos(todos: Todo[]) {
    list.replaceChildren(...todos.map(renderRow));
  }

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const title = input.value.trim();
    if (!title) return;
    const generation = ++latestSaveGeneration;
    pendingLocalSaveCount += 1;
    latestLocalSaveState = "saving";
    renderLocalSaveState();
    let savedLocally = false;
    try {
      const write = await db.insert(app.todos, { title, done: false });
      await write.wait({ tier: "local" });
      savedLocally = true;
      if (generation === latestSaveGeneration) latestLocalSaveState = "saved";
      pendingLocalSaveCount -= 1;
      renderLocalSaveState();
      await write.wait({ tier: "edge" });
      if (generation === latestSaveGeneration) form.reset();
    } catch {
      if (generation === latestSaveGeneration) {
        latestLocalSaveState = savedLocally ? "sync-failed" : "failed";
        renderLocalSaveState();
      }
    } finally {
      if (!savedLocally) {
        pendingLocalSaveCount -= 1;
        if (generation === latestSaveGeneration) latestLocalSaveState = "failed";
        renderLocalSaveState();
      }
    }
  });

  list.addEventListener("click", (event) => {
    const target = event.target as HTMLElement;
    const li = target.closest<HTMLLIElement>("li[data-id]");
    if (!li) return;
    const id = li.dataset.id!;
    if (target.dataset.action === "delete") {
      void Promise.resolve(db.delete(app.todos, id)).then((write) => write.wait({ tier: "edge" }));
    }
  });

  list.addEventListener("change", (event) => {
    const target = event.target as HTMLInputElement;
    if (target.dataset.action !== "toggle") return;
    const li = target.closest<HTMLLIElement>("li[data-id]");
    if (!li) return;
    void Promise.resolve(db.update(app.todos, li.dataset.id!, { done: target.checked })).then(
      (write) => write.wait({ tier: "edge" }),
    );
  });

  return db.subscribe(app.todos, (todos) => {
    // The simplest possible approach: rebuild the whole list on every tick.
    // It's fine here — the list is small and there's no DOM state to preserve
    // (no inline editing, no focused inputs inside rows).
    //
    renderTodos(todos);
  });
}
