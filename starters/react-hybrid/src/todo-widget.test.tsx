// @vitest-environment happy-dom
import React, { act, useState } from "react";
import { createRoot } from "react-dom/client";
import { expect, it, vi } from "vitest";

type Todo = { id: string; title: string; done: boolean };

function deferred<T>() {
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((_resolve, rejectPromise) => {
    reject = rejectPromise;
  });
  return { promise, reject };
}

const fixture = vi.hoisted(() => ({
  todos: [] as Array<{ id: string; title: string; done: boolean }>,
  refresh: () => {},
  db: { delete: vi.fn() },
}));

vi.mock("jazz-tools/react", () => ({
  useDb: () => fixture.db,
  useAll: () => ({ data: fixture.todos }),
}));
import { TodoWidget } from "./todo-widget";

function Harness() {
  const [, setRenderVersion] = useState(0);
  fixture.refresh = () => setRenderVersion((version) => version + 1);
  return <TodoWidget />;
}

async function mount(todo: Todo, deleteWrite: { wait: () => Promise<void> }) {
  fixture.todos = [todo];
  fixture.db.delete.mockImplementation(() => {
    fixture.todos = [];
    fixture.refresh();
    deleteWrite.wait().catch(() => {
      fixture.todos = [todo];
      fixture.refresh();
    });
    return deleteWrite;
  });

  const container = document.createElement("div");
  const root = createRoot(container);
  await act(async () => root.render(<Harness />));
  return { container, root };
}

it("shows a delete failure after an optimistic removal is rejected", async () => {
  const todo = { id: "todo-1", title: "Keep this task", done: false };
  const rejection = deferred<void>();
  const { container, root } = await mount(todo, { wait: () => rejection.promise });

  try {
    const deleteButton = container.querySelector<HTMLButtonElement>("button[aria-label='Delete']")!;
    await act(async () => deleteButton.click());
    expect(container.querySelector("li")).toBeNull();

    rejection.reject(new Error("delete denied"));
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(container.textContent).toContain(todo.title);
    const status = container.querySelector<HTMLElement>("[role='status']")!;
    expect(status.textContent).toContain("Delete failed");
    expect(status.textContent).not.toContain("Deleted");
    expect(status.textContent).not.toContain("Saved locally");
  } finally {
    await act(async () => root.unmount());
  }
});
