import { createJazzClient } from "jazz-tools/client";
import { app } from "../schema.js";

const errors = document.querySelector<HTMLParagraphElement>("#error")!;

async function main() {
  const session = await createJazzClient({ env: "dev", initial: "local-first" });
  const db = session.getSnapshot().client!.db;
  const form = document.querySelector<HTMLFormElement>("#add")!;
  const input = form.elements.namedItem("title") as HTMLInputElement;
  const add = form.querySelector<HTMLButtonElement>("button")!;
  const list = document.querySelector<HTMLUListElement>("#todos")!;
  let user = db.getAuthState().session?.user;
  add.disabled = !user;

  const unsubscribe = db.subscribe(app.todos, (todos) => {
    list.replaceChildren(
      ...todos.map((todo) => {
        const row = document.createElement("li");
        row.classList.toggle("done", todo.done);
        const toggle = document.createElement("input");
        toggle.type = "checkbox";
        toggle.checked = todo.done;
        toggle.disabled = todo.owner_id !== user;
        const title = document.createElement("span");
        title.textContent = todo.title;
        const remove = document.createElement("button");
        remove.textContent = "Delete";
        remove.disabled = todo.owner_id !== user;
        toggle.onchange = () => {
          try {
            db.update(app.todos, todo.id, { done: toggle.checked });
          } catch (error) {
            errors.textContent = String(error);
          }
        };
        remove.onclick = () => {
          try {
            db.delete(app.todos, todo.id);
          } catch (error) {
            errors.textContent = String(error);
          }
        };
        row.append(toggle, title, remove);
        return row;
      }),
    );
  });
  const stopAuth = db.onAuthChanged(({ session }) => {
    user = session?.user;
    add.disabled = !user;
  });
  form.onsubmit = (event) => {
    event.preventDefault();
    if (!user || !input.value.trim()) return;
    try {
      db.insert(app.todos, { title: input.value.trim(), done: false, owner_id: user });
      input.value = "";
      errors.textContent = "";
    } catch (error) {
      errors.textContent = String(error);
    }
  };
  if (import.meta.hot)
    import.meta.hot.dispose(() => {
      unsubscribe();
      stopAuth();
      void session.close();
    });
}

void main().catch((error) => {
  errors.textContent = String(error);
});
