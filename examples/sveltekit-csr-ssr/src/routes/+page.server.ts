import { app } from "$lib/schema";
import { backendDb, createServerSnapshot } from "$lib/server/jazz";
import type { Actions, PageServerLoad } from "./$types";

export const load: PageServerLoad = async () => {
  const db = backendDb();

  // Server-only column: read the current rows and render them directly.
  const serverTodos = await db.all(app.todos);

  // Hydrate column: prefetch the same query and dehydrate it into an envelope
  // the client seeds from on the first paint.
  const builder = createServerSnapshot();
  await builder.prefetch(db, app.todos);
  const snapshot = builder.dehydrate();

  return {
    serverTodos: serverTodos.map((todo) => ({ id: todo.id, title: todo.title })),
    snapshot,
  };
};

export const actions: Actions = {
  addServer: async ({ request }) => {
    const data = await request.formData();
    const title = (data.get("titleField") as string | null)?.trim();
    if (!title) return;
    // Wait for the write to settle globally so a re-read (and the other columns,
    // via sync) see it.
    await backendDb().insert(app.todos, { title, done: false }).wait({ tier: "global" });
    return { success: true };
  },
};
