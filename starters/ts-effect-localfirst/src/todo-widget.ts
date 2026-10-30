import { Effect, Fiber, Stream } from "effect";
import { Jazz, type JazzDb } from "jazz-tools/effect";
import { app } from "../schema.js";

type Todo = { id: string; title: string; done: boolean };

// The widget's Jazz operations, written against the `Jazz` service only. The
// same Effects run unchanged against a test database or on a server.

/** Insert a todo and complete once it is durable on this device. */
export const addTodo = (title: string) =>
  Jazz.use((jazz) => jazz.insert(app.todos, { title, done: false }, { wait: "local" }));

export const setTodoDone = (id: string, done: boolean) =>
  Jazz.use((jazz) => jazz.update(app.todos, id, { done }));

export const deleteTodo = (id: string) => Jazz.use((jazz) => jazz.delete(app.todos, id));

/** Every todo, as a new full snapshot whenever one changes. */
export const todos = Stream.unwrap(Jazz.useSync((jazz) => jazz.stream(app.todos)));

/** Writes that were saved locally but later rejected on their way to the server. */
export const syncFailures = Stream.unwrap(Jazz.useSync((jazz) => jazz.mutationErrors));

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

export function mountTodoWidget(parent: HTMLElement, jazz: JazzDb): () => void {
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

  // Run a widget Effect with this widget's database.
  const run = <A, E>(effect: Effect.Effect<A, E, Jazz>) =>
    Effect.runFork(Effect.provideService(effect, Jazz, jazz));

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

  form.addEventListener("submit", (event) => {
    event.preventDefault();
    const title = input.value.trim();
    if (!title) return;
    const generation = ++latestSaveGeneration;
    pendingLocalSaveCount += 1;
    latestLocalSaveState = "saving";
    renderLocalSaveState();
    run(
      addTodo(title).pipe(
        Effect.match({
          onSuccess: () => {
            if (generation !== latestSaveGeneration) return;
            latestLocalSaveState = "saved";
            form.reset();
          },
          onFailure: () => {
            if (generation === latestSaveGeneration) latestLocalSaveState = "failed";
          },
        }),
        Effect.ensuring(
          Effect.sync(() => {
            pendingLocalSaveCount -= 1;
            renderLocalSaveState();
          }),
        ),
      ),
    );
  });

  list.addEventListener("click", (event) => {
    const target = event.target as HTMLElement;
    const li = target.closest<HTMLLIElement>("li[data-id]");
    if (!li || target.dataset.action !== "delete") return;
    run(Effect.ignore(deleteTodo(li.dataset.id!)));
  });

  list.addEventListener("change", (event) => {
    const target = event.target as HTMLInputElement;
    if (target.dataset.action !== "toggle") return;
    const li = target.closest<HTMLLIElement>("li[data-id]");
    if (!li) return;
    run(Effect.ignore(setTodoDone(li.dataset.id!, target.checked)));
  });

  const live = run(
    Effect.all(
      [
        // The simplest possible approach: rebuild the whole list on every
        // snapshot. It's fine here — the list is small and there's no DOM
        // state to preserve (no inline editing, no focused inputs inside rows).
        todos.pipe(
          Stream.runForEach((rows) =>
            Effect.sync(() => list.replaceChildren(...rows.map(renderRow))),
          ),
        ),
        syncFailures.pipe(
          Stream.runForEach(() =>
            Effect.sync(() => {
              latestLocalSaveState = "sync-failed";
              renderLocalSaveState();
            }),
          ),
        ),
      ],
      { concurrency: "unbounded", discard: true },
    ).pipe(
      Effect.catchTag("JazzError", (error) =>
        Effect.sync(() => {
          localSaveStatus.textContent = error.message;
        }),
      ),
    ),
  );

  return () => {
    Effect.runFork(Fiber.interrupt(live));
  };
}
