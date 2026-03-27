import { revalidatePath } from "next/cache";
import { app } from "@/schema/app";
import { db } from "@/lib/jazz-server";

export default function ServerTodo() {
  return (
    <>
      <TodoForm />
      <TodoList />
    </>
  );
}

async function TodoList() {
  const todos = await db.all(app.todos);

  return (
    <ul className="mt-4 space-y-1">
      {todos.length === 0 && <li className="text-sm text-foreground/30 italic">No todos yet.</li>}
      {todos.map((todo) => (
        <li key={todo.id} className="text-sm py-1.5 border-b border-foreground/5 last:border-0">
          {todo.title}
        </li>
      ))}
    </ul>
  );
}

function TodoForm() {
  async function addTodo(formData: FormData) {
    "use server";
    const title = formData.get("titleField");
    if (typeof title !== "string" || !title.trim()) return;
    db.insert(app.todos, { title: title.trim(), done: false });
    revalidatePath("/");
  }

  return (
    <form action={addTodo} className="flex gap-2">
      <input
        name="titleField"
        type="text"
        placeholder="New todo…"
        className="flex-1 text-sm bg-transparent border border-foreground/15 rounded px-3 py-1.5 outline-none focus:border-foreground/40 placeholder:text-foreground/25"
      />
      <button
        type="submit"
        className="text-sm px-3 py-1.5 border border-foreground/15 rounded hover:bg-foreground/5 transition-colors cursor-pointer"
      >
        Add
      </button>
    </form>
  );
}
