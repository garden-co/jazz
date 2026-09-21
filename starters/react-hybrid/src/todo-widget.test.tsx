// @vitest-environment happy-dom
import React, { act, useState } from "react";
import { createRoot } from "react-dom/client";
import { expect, it, vi } from "vitest";

type Todo = { id: string; title: string; done: boolean };
type MutationErrorEvent = { reason: string; transaction: { transactionId: string } };
type DeleteWrite = {
  txId: Promise<string>;
  wait: (options?: { tier: "local" | "global" }) => Promise<void>;
};

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

const fixture = vi.hoisted(() => {
  const mutationErrorListeners = new Set<(event: MutationErrorEvent) => void>();
  return {
    todos: [] as Array<{ id: string; title: string; done: boolean }>,
    refresh: () => {},
    mutationErrorListeners,
    db: {
      delete: vi.fn(),
      onMutationError: vi.fn((listener: (event: MutationErrorEvent) => void) => {
        mutationErrorListeners.add(listener);
        return () => mutationErrorListeners.delete(listener);
      }),
    },
  };
});

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

async function mountTodos(
  todos: Todo[],
  deleteWrites: Map<string, DeleteWrite>,
  restoreOnReject = true,
) {
  fixture.todos = todos;
  fixture.db.delete.mockImplementation((_table: unknown, todoId: string) => {
    const todo = todos.find((candidate) => candidate.id === todoId)!;
    const deleteWrite = deleteWrites.get(todoId)!;
    fixture.todos = fixture.todos.filter((candidate) => candidate.id !== todoId);
    fixture.refresh();
    if (restoreOnReject) {
      deleteWrite.wait().catch(() => {
        if (!fixture.todos.some((candidate) => candidate.id === todo.id)) {
          fixture.todos = [...fixture.todos, todo];
          fixture.refresh();
        }
      });
    }
    return deleteWrite;
  });

  const container = document.createElement("div");
  const root = createRoot(container);
  await act(async () => root.render(<Harness />));
  return { container, root };
}

async function mount(todo: Todo, deleteWrite: DeleteWrite) {
  return mountTodos([todo], new Map([[todo.id, deleteWrite]]));
}

async function flushAsyncWork() {
  await act(async () => {
    await Promise.resolve();
    await Promise.resolve();
  });
}

it("keeps one delete failure visible when a concurrent delete succeeds", async () => {
  const first = { id: "todo-1", title: "Keep the first task", done: false };
  const second = { id: "todo-2", title: "Delete the second task", done: false };
  const firstRejection = deferred<void>();
  const secondCompletion = deferred<void>();
  const { container, root } = await mountTodos(
    [first, second],
    new Map([
      [first.id, { txId: Promise.resolve("tx-first"), wait: () => firstRejection.promise }],
      [second.id, { txId: Promise.resolve("tx-second"), wait: () => secondCompletion.promise }],
    ]),
  );
  try {
    const deleteButtons = container.querySelectorAll<HTMLButtonElement>(
      "button[aria-label='Delete']",
    );
    await act(async () => deleteButtons[0]!.click());
    await act(async () => deleteButtons[1]!.click());
    expect(container.querySelectorAll("li")).toHaveLength(0);

    firstRejection.reject(new Error("delete denied"));
    await flushAsyncWork();
    expect(container.textContent).toContain(first.title);
    expect(container.querySelector<HTMLElement>("[role='status']")!.textContent).toContain(
      first.title,
    );

    secondCompletion.resolve();
    await flushAsyncWork();

    const status = container.querySelector<HTMLElement>("[role='status']")!;
    expect(status.textContent).toContain("Delete failed");
    expect(status.textContent).toContain(first.title);
  } finally {
    await act(async () => root.unmount());
  }
});

it("keeps a delete failure visible while another delete starts", async () => {
  const first = { id: "todo-retained", title: "Keep the failed task", done: false };
  const second = { id: "todo-next", title: "Delete next", done: false };
  const firstRejection = deferred<void>();
  const secondCompletion = deferred<void>();
  const { container, root } = await mountTodos(
    [first, second],
    new Map([
      [first.id, { txId: Promise.resolve("tx-retained"), wait: () => firstRejection.promise }],
      [second.id, { txId: Promise.resolve("tx-next"), wait: () => secondCompletion.promise }],
    ]),
  );

  try {
    await act(async () => {
      container.querySelectorAll<HTMLButtonElement>("button[aria-label='Delete']")[0]!.click();
    });
    firstRejection.reject(new Error("delete denied"));
    await flushAsyncWork();
    expect(container.querySelector<HTMLElement>("[role='status']")!.textContent).toContain(
      first.title,
    );

    await act(async () => {
      container.querySelectorAll<HTMLButtonElement>("button[aria-label='Delete']")[0]!.click();
    });
    await flushAsyncWork();
    expect(container.querySelector<HTMLElement>("[role='status']")!.textContent).toContain(
      first.title,
    );

    secondCompletion.resolve();
    await flushAsyncWork();
    expect(container.querySelector<HTMLElement>("[role='status']")!.textContent).toContain(
      first.title,
    );
  } finally {
    await act(async () => root.unmount());
  }
});
it("completes local delete progress while edge confirmation is unavailable and reports late rejection", async () => {
  const todo = { id: "todo-transport", title: "Keep this deletion", done: false };
  const edgeFailure = new Error("transport unavailable");
  const { container, root } = await mountTodos(
    [todo],
    new Map([
      [
        todo.id,
        {
          txId: Promise.resolve("tx-transport"),
          wait: (options) =>
            options?.tier === "global" ? Promise.reject(edgeFailure) : Promise.resolve(),
        },
      ],
    ]),
    false,
  );

  try {
    await act(async () => {
      container.querySelector<HTMLButtonElement>("button[aria-label='Delete']")!.click();
    });
    await flushAsyncWork();

    expect(container.querySelector("li")).toBeNull();
    expect(container.querySelector<HTMLElement>("[role='status']")!.textContent).not.toContain(
      "Delete failed",
    );
    expect(container.querySelector("[role='status']")!.textContent).not.toContain("Deleting…");
    for (const listener of fixture.mutationErrorListeners) {
      listener({ reason: "delete denied", transaction: { transactionId: "tx-transport" } });
    }
    await flushAsyncWork();
    expect(container.querySelector("li")).toBeNull();
    expect(container.querySelector<HTMLElement>("[role='status']")!.textContent).toContain(
      "A change was rejected: delete denied",
    );
  } finally {
    await act(async () => root.unmount());
  }
});

it("shows a delete failure after an optimistic removal is rejected", async () => {
  const todo = { id: "todo-1", title: "Keep this task", done: false };
  const rejection = deferred<void>();
  const { container, root } = await mount(todo, {
    txId: Promise.resolve("tx-local"),
    wait: () => rejection.promise,
  });
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

it("preserves a late mutation rejection through unrelated delete success and unsubscribes", async () => {
  const todo = { id: "other", title: "Other task", done: false };
  const { container, root } = await mount(todo, {
    txId: Promise.resolve("other-tx"),
    wait: () => Promise.resolve(),
  });
  const listeners = [...fixture.mutationErrorListeners];
  try {
    await act(async () => {
      for (const listener of listeners)
        listener({ reason: "permission denied", transaction: { transactionId: "unrelated-tx" } });
    });
    await act(async () =>
      container.querySelector<HTMLButtonElement>("button[aria-label='Delete']")!.click(),
    );
    expect(container.querySelector("[role='status']")!.textContent).toBe(
      "A change was rejected: permission denied",
    );
  } finally {
    await act(async () => root.unmount());
  }
  for (const listener of listeners)
    expect(fixture.mutationErrorListeners.has(listener)).toBe(false);
});

it("does not let an older same-row attempt replace a retry failure", async () => {
  const todo = { id: "retry", title: "Retry task", done: false };
  const old = deferred<void>();
  const retry = deferred<void>();
  const writes = new Map([[todo.id, { txId: Promise.resolve("old"), wait: () => old.promise }]]);
  const { container, root } = await mountTodos([todo], writes, false);
  try {
    await act(async () =>
      container.querySelector<HTMLButtonElement>("button[aria-label='Delete']")!.click(),
    );
    // A subscription restores the row before the older local waiter completes.
    await act(async () => {
      fixture.todos = [todo];
      fixture.refresh();
    });
    writes.set(todo.id, { txId: Promise.resolve("retry"), wait: () => retry.promise });
    await act(async () =>
      container.querySelector<HTMLButtonElement>("button[aria-label='Delete']")!.click(),
    );
    old.reject(new Error("old failure"));
    await flushAsyncWork();
    expect(container.querySelector("[role='status']")!.textContent).toBe("Deleting…");
    retry.reject(new Error("retry failure"));
    await flushAsyncWork();
    expect(container.querySelector("[role='status']")!.textContent).toBe(
      "Delete failed: Retry task",
    );
  } finally {
    await act(async () => root.unmount());
  }
});

it("ignores old local completions after the database lifecycle changes", async () => {
  const todo = { id: "old-lifecycle", title: "Old task", done: false };
  const old = deferred<void>();
  const { container, root } = await mountTodos(
    [todo],
    new Map([[todo.id, { txId: Promise.resolve("old"), wait: () => old.promise }]]),
    false,
  );
  const originalDb = fixture.db;
  const oldListeners = [...fixture.mutationErrorListeners];
  try {
    await act(async () =>
      container.querySelector<HTMLButtonElement>("button[aria-label='Delete']")!.click(),
    );
    await act(async () => {
      fixture.db = { ...originalDb };
      fixture.refresh();
    });
    old.reject(new Error("old local failure"));
    await act(async () => {
      for (const listener of oldListeners)
        listener({ reason: "stale", transaction: { transactionId: "old" } });
    });
    await flushAsyncWork();
    expect(container.querySelector("[role='status']")!.textContent).toBe("Ready to save locally");
  } finally {
    await act(async () => root.unmount());
    fixture.db = originalDb;
  }
});
