import { defineSuite } from "../runner/harness";
import { app } from "../schema";

type TodoWithProject = { id: string; title: string; project: { name: string } | null };

export default defineSuite("relations", ({ test }) => {
  test("includes a forward ref relation", async ({ db, expect, waitForQuery }) => {
    const { value: project } = db.insert(app.projects, { name: "Acme" });
    const { value: todo } = db.insert(app.todos, {
      title: "ship it",
      done: false,
      projectId: project.id,
    });

    const rows = await waitForQuery<TodoWithProject>(
      db,
      app.todos.where({ id: { eq: todo.id } }).include({ project: true }),
      (r) => r.length === 1 && r[0]!.project != null,
      "todo with project included",
    );
    expect(rows[0]!.project?.name).toBe("Acme");
  });
});
