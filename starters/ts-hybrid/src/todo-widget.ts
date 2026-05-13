import type { Db } from "jazz-tools";
import { app } from "../schema.js";

type Todo = { id: string; title: string; done: boolean };

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

export function mountTodoWidget(parent: HTMLElement, db: Db): () => void {
  parent.innerHTML = `
    <section class="todo-widget">
      <h2>Your todos</h2>
      <form>
        <input type="text" name="title" placeholder="Add a task" aria-label="New todo" />
        <button type="submit">Add</button>
      </form>
      <ul></ul>
    </section>
  `;
  const form = parent.querySelector<HTMLFormElement>("form")!;
  const input = form.querySelector<HTMLInputElement>("input[name='title']")!;
  const list = parent.querySelector<HTMLUListElement>("ul")!;

  form.addEventListener("submit", (event) => {
    event.preventDefault();
    const title = input.value.trim();
    if (!title) return;
    db.insert(app.todos, { title, done: false });
    form.reset();
  });

  list.addEventListener("click", (event) => {
    const target = event.target as HTMLElement;
    const li = target.closest<HTMLLIElement>("li[data-id]");
    if (!li) return;
    const id = li.dataset.id!;
    if (target.dataset.action === "delete") {
      db.delete(app.todos, id);
    }
  });

  list.addEventListener("change", (event) => {
    const target = event.target as HTMLInputElement;
    if (target.dataset.action !== "toggle") return;
    const li = target.closest<HTMLLIElement>("li[data-id]");
    if (!li) return;
    db.update(app.todos, li.dataset.id!, { done: target.checked });
  });

  return db.subscribeAll(app.todos, (delta) => {
    // The simplest possible approach: rebuild the whole list on every tick.
    // It's fine here — the list is small and there's no DOM state to preserve
    // (no inline editing, no focused inputs inside rows).
    //
    // If you need finer-grained updates, the delta gives you everything:
    //   delta.all   — the full current result set (Todo[]) after this tick
    //   delta.delta — ordered row-level changes:
    //       { kind: Added,   id, index, item }   // new row at `index`
    //       { kind: Updated, id, index, item }   // row content changed
    //       { kind: Removed, id, index }         // row gone at `index`
    // Iterate delta.delta to apply per-row DOM patches instead of a full
    // swap, e.g. to keep focus, preserve animations, or avoid reflow cost.
    list.replaceChildren(...delta.all.map(renderRow));
  });
}
