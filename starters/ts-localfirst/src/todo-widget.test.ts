import { expect, test } from "vitest";
import { mountTodoWidget, type TodoDb } from "./todo-widget.js";

type TestTodo = { id: string; title: string; done: boolean };

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function localAndEdgeWrites() {
  const local = deferred<void>();
  const edge = deferred<void>();
  const write = {
    value: undefined,
    wait({ tier }: { tier?: "local" | "edge" | "global" } = {}) {
      return tier === "edge" ? edge.promise : local.promise;
    },
  };
  return { local, edge, write };
}

function settledWrite<T = void>() {
  return {
    value: undefined as T,
    wait: async () => undefined as T,
  };
}

async function flush() {
  await Promise.resolve();
  await Promise.resolve();
}

function mount(db: Omit<TodoDb, "subscribe">) {
  const parent = document.createElement("div");
  let publishTodos: (todos: TestTodo[]) => void = () => {};
  const subscribe: TodoDb["subscribe"] = (_query, callback) => {
    callback([]);
    publishTodos = callback as unknown as (todos: TestTodo[]) => void;
    return () => {};
  };
  mountTodoWidget(parent, { ...db, subscribe });
  return {
    form: parent.querySelector<HTMLFormElement>("form")!,
    input: parent.querySelector<HTMLInputElement>("input")!,
    status: parent.querySelector<HTMLElement>("[role='status']")!,
    publishTodos,
  };
}

function submit(form: HTMLFormElement, input: HTMLInputElement, title: string) {
  input.value = title;
  form.requestSubmit();
}

test("a rejected local write reports failure without leaking its rejection", async () => {
  const local = deferred<void>();
  const { form, input, status } = mount({
    insert: <T>() => ({
      value: undefined as T,
      wait: () => local.promise as Promise<T>,
    }),
    update: () => ({ value: undefined, wait: async () => undefined }),
    delete: () => ({ value: undefined, wait: async () => undefined }),
  });

  submit(form, input, "will fail");
  local.reject(new Error("disk unavailable"));
  await flush();

  expect(status.textContent).toBe("Save failed locally");
  expect(input.value).toBe("will fail");
});

test("overlapping adds do not let an older completion announce local durability", async () => {
  const first = localAndEdgeWrites();
  const second = localAndEdgeWrites();
  const writes = [first.write, second.write];
  const { form, input, status } = mount({
    insert: <T>() => writes.shift()! as ReturnType<typeof settledWrite<T>>,
    update: settledWrite,
    delete: settledWrite,
  });

  submit(form, input, "A");
  submit(form, input, "B");
  first.local.resolve();
  await flush();
  expect(status.textContent).toBe("Saving locally…");

  second.local.resolve();
  await flush();
  expect(status.textContent).toBe("Saved locally");

  first.edge.resolve();
  second.edge.resolve();
  await flush();
});

test("the local marker precedes edge durability, which alone resets the form", async () => {
  const write = localAndEdgeWrites();
  const { form, input, status, publishTodos } = mount({
    insert: <T>() => write.write as ReturnType<typeof settledWrite<T>>,
    update: settledWrite,
    delete: settledWrite,
  });

  submit(form, input, "wait for edge");
  publishTodos([{ id: "optimistic", title: "wait for edge", done: false }]);
  expect(form.parentElement?.textContent).toContain("wait for edge");
  write.local.resolve();
  await flush();
  expect(status.textContent).toBe("Saved locally");
  expect(input.value).toBe("wait for edge");

  write.edge.resolve();
  await flush();
  expect(input.value).toBe("");
});

test("an older edge completion cannot reset a newer pending add", async () => {
  const first = localAndEdgeWrites();
  const second = localAndEdgeWrites();
  const writes = [first.write, second.write];
  const { form, input } = mount({
    insert: <T>() => writes.shift()! as ReturnType<typeof settledWrite<T>>,
    update: settledWrite,
    delete: settledWrite,
  });

  submit(form, input, "A");
  first.local.resolve();
  await flush();
  submit(form, input, "B");

  first.edge.resolve();
  await flush();
  expect(input.value).toBe("B");

  second.local.resolve();
  second.edge.resolve();
  await flush();
  expect(input.value).toBe("");
});

test("an edge rejection keeps the local acknowledgement and reports sync failure", async () => {
  const write = localAndEdgeWrites();
  const { form, input, status } = mount({
    insert: <T>() => write.write as ReturnType<typeof settledWrite<T>>,
    update: settledWrite,
    delete: settledWrite,
  });

  submit(form, input, "edge failure");
  write.local.resolve();
  await flush();
  write.edge.reject(new Error("edge unavailable"));
  await flush();

  expect(status.textContent).toBe("Saved locally; sync failed");
  expect(input.value).toBe("edge failure");
});
