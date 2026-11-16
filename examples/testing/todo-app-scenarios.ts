/** Each example supplies its test runner, app mount, and rendering flushes. */
export function todoCrudTest({
  it,
  expect,
  mount,
  waitFor,
  submit = submitTodo,
  click = (element) => element.click(),
}: {
  it: (name: string, test: () => Promise<void>) => unknown;
  expect: (actual: unknown) => {
    toBe: (expected: unknown) => void;
    toBeTruthy: () => void;
    toHaveLength: (length: number) => void;
    toEqual: (expected: unknown) => void;
  };
  mount: () => Promise<HTMLDivElement>;
  waitFor: (check: () => boolean, timeoutMs: number, message: string) => Promise<void>;
  submit?: (container: HTMLDivElement, title: string) => void | Promise<void>;
  click?: (element: HTMLElement) => void | Promise<void>;
}): void {
  it("renders, adds, toggles, deletes, and lists todos", async () => {
    const el = await mount();
    const rows = () => el.querySelectorAll("#todo-list li");
    expect(el.querySelector("h1")!.textContent).toBe("Todos");
    expect(el.querySelector("#todo-list")).toBeTruthy();
    expect(rows()).toHaveLength(0);

    await submit(el, "Buy milk");
    await waitFor(() => rows().length === 1, 3000, "Todo should appear after form submit");
    expect(rows()[0].querySelector("span")!.textContent).toBe("Buy milk");
    expect(rows()[0].classList.contains("done")).toBe(false);

    // Delete an open todo: several examples only let open todos be deleted
    // (`allowDelete.where({ done: false })`), which a thin client hears from
    // the server at once.
    await click(el.querySelector<HTMLButtonElement>(".delete-btn")!);
    await waitFor(() => rows().length === 0, 3000, "Todo should be removed");

    await submit(el, "Walk the dog");
    await waitFor(() => rows().length === 1, 3000, "Todo should appear after form submit");
    await click(el.querySelector<HTMLInputElement>("input.toggle")!);
    await waitFor(
      () => rows()[0]?.classList.contains("done") ?? false,
      5000,
      "Todo should be marked done",
    );

    const titles = ["First", "Second", "Third"];
    for (const [index, title] of titles.entries()) {
      await submit(el, title);
      await waitFor(() => rows().length === index + 2, 3000, `Todo ${title} should appear`);
    }
    expect(
      [...el.querySelectorAll("#todo-list li span")].map((span) => span.textContent).sort(),
    ).toEqual([...titles, "Walk the dog"].sort());
  });
}

function submitTodo(container: HTMLDivElement, title: string): void {
  const input = container.querySelector<HTMLInputElement>("input[type='text']")!;
  input.value = title;
  input.dispatchEvent(new Event("input", { bubbles: true }));
  input.closest("form")!.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
}
