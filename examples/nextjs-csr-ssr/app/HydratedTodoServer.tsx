import { app } from "../schema";
import { db, createServerSnapshot } from "@/lib/jazz-server";
import HydratedTodoClient from "./HydratedTodoClient";

// Use `JazzContext.forSession(session)` to get a prefetch which runs as the
// client to pre-seed data scoped for the user. Be *extremely* cautious to
// avoid caching private data.
export default async function HydratedTodoServer() {
  const builder = createServerSnapshot();
  await builder.prefetch(db, app.todos);
  const snapshot = builder.dehydrate();

  return (
    <>
      <TodoForm />
      <HydratedTodoClient snapshot={snapshot} />
    </>
  );
}

function TodoForm() {
  async function addTodo(formData: FormData) {
    "use server";
    const title = formData.get("titleField");
    if (typeof title !== "string" || !title.trim()) return;
    await db.insert(app.todos, { title: title.trim(), done: false }).wait({ tier: "global" });
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
