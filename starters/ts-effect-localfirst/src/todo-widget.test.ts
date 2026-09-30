import { Effect, Stream } from "effect";
import { JazzError, type JazzDb } from "jazz-tools/effect";
import { expect, test } from "vitest";
import { mountTodoWidget } from "./todo-widget.js";

type TestTodo = { id: string; title: string; done: boolean };

// Only the operations the widget uses.
function fakeJazz(overrides: Partial<JazzDb>): JazzDb {
  const base: Partial<JazzDb> = {
    stream: (() => Stream.make([] as TestTodo[])) as unknown as JazzDb["stream"],
    mutationErrors: Stream.never,
    insert: (() => Effect.succeed({ id: "new" })) as unknown as JazzDb["insert"],
    update: () => Effect.void,
    delete: () => Effect.void,
  };
  return { ...base, ...overrides } as JazzDb;
}

function mount(jazz: JazzDb) {
  const parent = document.createElement("div");
  const unmount = mountTodoWidget(parent, jazz);
  return {
    parent,
    unmount,
    form: parent.querySelector<HTMLFormElement>("form")!,
    input: parent.querySelector<HTMLInputElement>("input")!,
    status: parent.querySelector<HTMLElement>("[role='status']")!,
  };
}

function submit(form: HTMLFormElement, input: HTMLInputElement, title: string) {
  input.value = title;
  form.requestSubmit();
}

test("renders every snapshot from the live query", async () => {
  const { parent, unmount } = mount(
    fakeJazz({
      stream: (() =>
        Stream.make([{ id: "a", title: "Buy milk", done: false }])) as unknown as JazzDb["stream"],
    }),
  );
  await expect.poll(() => parent.querySelectorAll("li").length).toBe(1);
  expect(parent.querySelector("li")?.textContent).toContain("Buy milk");
  unmount();
});

test("a local save is acknowledged and clears the form", async () => {
  const inserted: unknown[] = [];
  const { form, input, status, unmount } = mount(
    fakeJazz({
      insert: ((_table: unknown, data: unknown, options: unknown) => {
        inserted.push({ data, options });
        return Effect.succeed({ id: "new" });
      }) as unknown as JazzDb["insert"],
    }),
  );
  submit(form, input, "Walk the dog");
  await expect.poll(() => status.textContent).toBe("Saved locally");
  expect(input.value).toBe("");
  expect(inserted).toEqual([
    { data: { title: "Walk the dog", done: false }, options: { wait: "local" } },
  ]);
  unmount();
});

test("a failed local save is reported and keeps the input", async () => {
  const { form, input, status, unmount } = mount(
    fakeJazz({
      insert: (() =>
        Effect.fail(
          new JazzError({ operation: "insert", cause: new Error("disk full") }),
        )) as unknown as JazzDb["insert"],
    }),
  );
  submit(form, input, "Walk the dog");
  await expect.poll(() => status.textContent).toBe("Save failed locally");
  expect(input.value).toBe("Walk the dog");
  unmount();
});

test("a write rejected after saving locally is reported as a sync failure", async () => {
  const { status, unmount } = mount(
    fakeJazz({ mutationErrors: Stream.make({}) as unknown as JazzDb["mutationErrors"] }),
  );
  await expect.poll(() => status.textContent).toBe("Saved locally; sync failed");
  unmount();
});
