import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { TableDataGrid } from "./TableDataGrid";

const mockUseAll = vi.fn();
const mockUpdateDurable = vi.fn();
const mockInsertDurable = vi.fn();
const mockDeleteDurable = vi.fn();
let currentRows: Array<Record<string, unknown>>;
let currentReferenceRowsByTable: Record<string, Array<Record<string, unknown>>>;

function getContainingCell(element: HTMLElement | null): HTMLElement | null {
  return element?.closest('[role="gridcell"], td') ?? null;
}

function getContainingRow(element: HTMLElement | null): HTMLElement | null {
  return element?.closest('[role="row"], tr') ?? null;
}

function getLastTodosQuery(): { _build: () => string } {
  return [...mockUseAll.mock.calls]
    .reverse()
    .map((call) => call[0] as { _build: () => string })
    .find((query) => {
      if (!query || typeof query !== "object" || !("_build" in query)) return false;
      return JSON.parse(query._build()).table === "todos";
    })!;
}

function renderGridUi() {
  return (
    <MemoryRouter initialEntries={["/data-explorer/todos/data"]}>
      <TableDataGrid />
    </MemoryRouter>
  );
}

function renderGrid() {
  return render(renderGridUi());
}

const mockWasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
      { name: "meta", column_type: { type: "Row", columns: [] }, nullable: true },
      { name: "owner_id", column_type: { type: "Uuid" }, nullable: true, references: "users" },
      { name: "blob", column_type: { type: "Bytea" }, nullable: true },
    ],
  },
  users: {
    columns: [
      { name: "displayName", column_type: { type: "Text" }, nullable: false },
      { name: "email", column_type: { type: "Text" }, nullable: false },
    ],
  },
};

vi.mock("jazz-tools/react", () => ({
  useAll: (...args: unknown[]) => mockUseAll(...args),
  useDb: () => ({
    updateDurable: (...args: unknown[]) => mockUpdateDurable(...args),
    insertDurable: (...args: unknown[]) => mockInsertDurable(...args),
    deleteDurable: (...args: unknown[]) => mockDeleteDurable(...args),
  }),
}));

vi.mock("../../contexts/devtools-context.js", () => ({
  useDevtoolsContext: () => ({
    wasmSchema: mockWasmSchema,
    runtime: "extension",
    queryPropagation: "local-only",
  }),
}));

vi.mock("react-router", async () => {
  const actual = await vi.importActual<typeof import("react-router")>("react-router");
  return {
    ...actual,
    useParams: () => ({ table: "todos" }),
  };
});

describe("TableDataGrid", () => {
  afterEach(() => {
    cleanup();
    vi.useRealTimers();
  });

  beforeEach(() => {
    currentRows = [
      {
        id: "row-2",
        title: "zeta",
        done: false,
        meta: { done: true },
        owner_id: "owner-a",
        blob: new Uint8Array([1, 2]),
      },
      {
        id: "row-1",
        title: "alpha",
        done: true,
        meta: null,
        owner_id: "owner-b",
        blob: new Uint8Array([5, 6]),
      },
    ];
    currentReferenceRowsByTable = {
      users: [
        { id: "owner-a", displayName: "Alice", email: "alice@example.test" },
        { id: "owner-b", displayName: "Bob", email: "bob@example.test" },
      ],
    };

    mockUpdateDurable.mockReset();
    mockInsertDurable.mockReset();
    mockDeleteDurable.mockReset();
    mockUpdateDurable.mockResolvedValue(undefined);
    mockInsertDurable.mockResolvedValue({ id: "new-row" });
    mockDeleteDurable.mockResolvedValue(undefined);
    mockUseAll.mockReset();
    mockUseAll.mockImplementation((query) => {
      const builtQuery =
        query &&
        typeof query === "object" &&
        "_build" in query &&
        typeof query._build === "function"
          ? JSON.parse(query._build())
          : null;

      if (!builtQuery || builtQuery.table === "todos") {
        return currentRows;
      }

      const tableRows = currentReferenceRowsByTable[builtQuery.table] ?? [];
      return tableRows.filter((row) =>
        builtQuery.conditions.every((condition: { column: string; op: string; value: unknown }) => {
          if (condition.op !== "eq") {
            return true;
          }
          return row[condition.column] === condition.value;
        }),
      );
    });
  });

  it("renders schema-derived columns and reactive rows", () => {
    renderGrid();

    expect(screen.getByRole("heading", { name: "todos" })).not.toBeNull();
    expect(screen.queryByText("6 columns · 2 rows on page · 0 filters")).toBeNull();
    const header = screen.getByRole("heading", { name: "todos" }).closest("header");
    expect(header).not.toBeNull();
    expect(within(header as HTMLElement).getByRole("button", { name: "Filter" })).not.toBeNull();
    expect(within(header as HTMLElement).getByRole("button", { name: "Insert" })).not.toBeNull();
    expect(screen.getByRole("columnheader", { name: /ID/ })).not.toBeNull();
    expect(screen.getByRole("columnheader", { name: "title" })).not.toBeNull();
    expect(screen.getByRole("columnheader", { name: "done" })).not.toBeNull();
    expect(screen.getByRole("columnheader", { name: "meta" })).not.toBeNull();
    expect(screen.getByText("row-2")).not.toBeNull();
    expect(screen.getByText("zeta")).not.toBeNull();
    expect(screen.getByText('{"done":true}')).not.toBeNull();
    expect((screen.getByLabelText("Rows per page") as HTMLSelectElement).value).toBe("25");
  });

  it("renders reference cells as links to the related table filtered by id", () => {
    renderGrid();

    expect(screen.getByText("Alice")).not.toBeNull();
    const relationLink = screen.getByRole("link", { name: "Open Alice in users" });
    const href = relationLink.getAttribute("href");
    expect(href).not.toBeNull();

    const url = new URL(href!, "https://inspector.test");
    expect(url.pathname).toBe("/data-explorer/users/data");
    expect(JSON.parse(url.searchParams.get("filters") ?? "[]")).toMatchObject([
      {
        column: "id",
        operator: "eq",
        value: "owner-a",
      },
    ]);
  });

  it("keeps the edit sidebar visible even when no row is selected", () => {
    renderGrid();

    expect(screen.getByRole("heading", { name: "Edit row" })).not.toBeNull();
    expect(screen.getByText("Select a row from the table to edit it.")).not.toBeNull();
    expect(screen.getAllByRole("separator")).toHaveLength(1);
  });

  it("updates query sorting when a sortable column header is clicked", () => {
    renderGrid();

    const firstQuery = mockUseAll.mock.calls[0]?.[0] as { _build: () => string };
    expect(JSON.parse(firstQuery._build())).toMatchObject({
      orderBy: [["id", "asc"]],
      limit: 26,
      offset: 0,
    });

    const titleHeader = screen.getByRole("columnheader", { name: "title" });
    fireEvent.click(titleHeader);

    const sortedQuery = getLastTodosQuery();
    expect(JSON.parse(sortedQuery._build())).toMatchObject({
      orderBy: [["title", "asc"]],
      limit: 26,
      offset: 0,
    });
  });

  it("subscribes with local-only propagation in extension mode", () => {
    renderGrid();

    expect(mockUseAll).toHaveBeenCalledWith(
      expect.any(Object),
      expect.objectContaining({
        propagation: "local-only",
        visibility: "hidden_from_live_query_list",
      }),
    );
  });

  it("adds a where clause and compiles it into query conditions", () => {
    renderGrid();

    fireEvent.click(screen.getByRole("button", { name: /Filter/ }));
    fireEvent.change(screen.getByLabelText("Column"), { target: { value: "title" } });
    fireEvent.change(screen.getByLabelText("Operator"), { target: { value: "contains" } });
    fireEvent.change(screen.getByLabelText("Value"), { target: { value: "alpha" } });
    fireEvent.click(screen.getByRole("button", { name: "Add where clause" }));

    const filteredQuery = getLastTodosQuery();
    expect(JSON.parse(filteredQuery._build())).toMatchObject({
      conditions: [{ column: "title", op: "contains", value: "alpha" }],
      orderBy: [["id", "asc"]],
      limit: 26,
      offset: 0,
    });
  });

  it("opens row edit sidebar and updates editable fields", async () => {
    renderGrid();

    fireEvent.click(screen.getByRole("gridcell", { name: "zeta" }));

    await waitFor(() => {
      expect(screen.getByDisplayValue("row-2")).not.toBeNull();
    });

    expect(screen.getByRole("heading", { name: "Edit row" })).not.toBeNull();
    expect(screen.getByText("Read-only: binary field")).not.toBeNull();

    fireEvent.change(screen.getByLabelText("title"), { target: { value: "zeta updated" } });
    const doneField = screen.getByLabelText("done field");
    fireEvent.change(within(doneField).getByRole("combobox"), { target: { value: "true" } });
    const ownerField = screen.getByLabelText("owner_id field");
    fireEvent.change(within(ownerField).getByRole("textbox"), { target: { value: "owner-c" } });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    expect(mockUpdateDurable).toHaveBeenCalledWith(
      expect.objectContaining({ _table: "todos" }),
      "row-2",
      expect.objectContaining({
        title: "zeta updated",
        done: true,
        owner_id: "owner-c",
      }),
      { tier: "worker" },
    );
  });

  it("preserves unsaved edits when the current row live-updates", async () => {
    const { rerender } = renderGrid();

    fireEvent.click(screen.getByRole("gridcell", { name: "zeta" }));

    await waitFor(() => {
      expect(screen.getByDisplayValue("row-2")).not.toBeNull();
    });

    fireEvent.change(screen.getByLabelText("title"), { target: { value: "local draft" } });

    currentRows = [{ ...currentRows[0], title: "server pushed update" }, currentRows[1]!];
    rerender(renderGridUi());

    expect((screen.getByLabelText("title") as HTMLInputElement).value).toBe("local draft");
  });

  it("uses the same editable sidebar for selected rows and saves changes from it", async () => {
    renderGrid();

    fireEvent.click(screen.getByRole("gridcell", { name: "zeta" }));

    await waitFor(() => {
      expect(screen.getByDisplayValue("row-2")).not.toBeNull();
    });
    expect(screen.getByRole("heading", { name: "Edit row" })).not.toBeNull();
    expect(screen.getAllByRole("separator")).toHaveLength(1);
    expect(screen.getByDisplayValue("zeta")).not.toBeNull();
    const doneField = screen.getByLabelText("done field");
    expect((within(doneField).getByRole("combobox") as HTMLSelectElement).value).toBe("false");

    fireEvent.change(screen.getByLabelText("title"), { target: { value: "selected row edit" } });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => {
      expect(mockUpdateDurable).toHaveBeenCalledWith(
        expect.objectContaining({ _table: "todos" }),
        "row-2",
        expect.objectContaining({
          title: "selected row edit",
          done: false,
          meta: { done: true },
          owner_id: "owner-a",
        }),
        { tier: "worker" },
      );
    });
  });

  it("clears the selected row on escape while keeping the edit sidebar visible", async () => {
    renderGrid();

    fireEvent.click(screen.getByRole("gridcell", { name: "zeta" }));

    await waitFor(() => {
      expect(screen.getByDisplayValue("row-2")).not.toBeNull();
    });

    fireEvent.keyDown(window, { key: "Escape" });

    await waitFor(() => {
      expect(screen.getByRole("heading", { name: "Edit row" })).not.toBeNull();
      expect(screen.queryByDisplayValue("row-2")).toBeNull();
      expect(screen.getByText("Select a row from the table to edit it.")).not.toBeNull();
    });
  });

  it("queues inline cell edits on double click and saves them from the banner", async () => {
    renderGrid();

    fireEvent.doubleClick(screen.getByRole("gridcell", { name: "zeta" }));
    const editor = screen.getByLabelText("Edit title");
    fireEvent.change(editor, { target: { value: "zeta queued" } });
    fireEvent.blur(editor);

    expect(screen.getByText("Queued")).not.toBeNull();
    expect(screen.getByText("1 edit across 1 row")).not.toBeNull();
    expect(screen.getByText("zeta queued")).not.toBeNull();
    expect(mockUpdateDurable).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("button", { name: "Save changes" }));

    await waitFor(() => {
      expect(mockUpdateDurable).toHaveBeenCalledWith(
        expect.objectContaining({ _table: "todos" }),
        "row-2",
        expect.objectContaining({
          title: "zeta queued",
        }),
        { tier: "worker" },
      );
    });

    expect(screen.queryByText(/queued change across/i)).toBeNull();
  });

  it("renders boolean table cells as always-on checkboxes and saves queued toggles", async () => {
    renderGrid();

    const checkbox = screen.getByRole("checkbox", { name: "Toggle done for row-2" });
    expect((checkbox as HTMLInputElement).checked).toBe(false);

    fireEvent.click(checkbox);

    expect(screen.getByText("Queued")).not.toBeNull();
    expect(screen.getByText("1 edit across 1 row")).not.toBeNull();
    expect(mockUpdateDurable).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("button", { name: "Save changes" }));

    await waitFor(() => {
      expect(mockUpdateDurable).toHaveBeenCalledWith(
        expect.objectContaining({ _table: "todos" }),
        "row-2",
        expect.objectContaining({
          done: true,
        }),
        { tier: "worker" },
      );
    });
  });

  it("does not queue row deletion when backspace is pressed inside an active cell editor", () => {
    render(
      <MemoryRouter initialEntries={["/data-explorer/todos/data"]}>
        <TableDataGrid />
      </MemoryRouter>,
    );

    fireEvent.doubleClick(screen.getByRole("gridcell", { name: "zeta" }));

    const editor = screen.getByLabelText("Edit title");
    fireEvent.keyDown(editor, { key: "Backspace" });

    expect(screen.queryByText(/row will be deleted/i)).toBeNull();
    expect(screen.getByLabelText("Edit title")).not.toBeNull();
  });

  it("caps data column width so long cell values do not stretch the whole grid", () => {
    renderGrid();

    const titleMeasuringCell = document.querySelector(
      '[data-measuring-cell-key="title"]',
    ) as HTMLElement | null;
    expect(titleMeasuringCell).not.toBeNull();
    expect(titleMeasuringCell?.style.maxWidth).toBe("360px");
  });

  it("renders without frozen columns so actions stay last and id scrolls normally", () => {
    renderGrid();

    expect(document.querySelector(".rdg-cell-frozen")).toBeNull();
  });

  it("marks changed cells so live updates can pulse", () => {
    vi.useFakeTimers();
    const { rerender } = renderGrid();

    currentRows = [{ ...currentRows[0], title: "zeta updated live" }, currentRows[1]!];
    rerender(renderGridUi());

    const changedCell = getContainingCell(screen.getByText("zeta updated live"));
    expect(changedCell?.getAttribute("data-cell-change-state")).toBe("updated");

    act(() => {
      vi.advanceTimersByTime(1_300);
    });

    expect(getContainingCell(screen.getByText("zeta updated live"))?.dataset.cellChangeState).toBe(
      undefined,
    );
  });

  it("highlights rows that were inserted by a live update", () => {
    vi.useFakeTimers();
    const { rerender } = renderGrid();

    currentRows = [
      {
        id: "row-3",
        title: "brand new",
        done: false,
        meta: null,
        owner_id: "owner-c",
        blob: null,
      },
      ...currentRows,
    ];
    rerender(renderGridUi());

    expect(getContainingRow(screen.getByText("row-3"))?.getAttribute("data-row-change-state")).toBe(
      "added",
    );

    act(() => {
      vi.advanceTimersByTime(2_100);
    });

    expect(getContainingRow(screen.getByText("row-3"))?.dataset.rowChangeState).toBe(undefined);
  });

  it("does not highlight rows when the first result set loads", () => {
    currentRows = [];
    const { rerender } = renderGrid();

    currentRows = [
      {
        id: "row-3",
        title: "brand new",
        done: false,
        meta: null,
        owner_id: "owner-c",
        blob: null,
      },
      {
        id: "row-2",
        title: "zeta",
        done: false,
        meta: { done: true },
        owner_id: "owner-a",
        blob: new Uint8Array([1, 2]),
      },
      {
        id: "row-1",
        title: "alpha",
        done: true,
        meta: null,
        owner_id: "owner-b",
        blob: new Uint8Array([5, 6]),
      },
    ];
    rerender(renderGridUi());

    expect(getContainingRow(screen.getByText("row-3"))?.dataset.rowChangeState).toBe(undefined);
    expect(getContainingRow(screen.getByText("row-2"))?.dataset.rowChangeState).toBe(undefined);
    expect(getContainingRow(screen.getByText("row-1"))?.dataset.rowChangeState).toBe(undefined);
  });

  it("keeps removed rows around briefly so they can animate out", () => {
    vi.useFakeTimers();
    const { rerender } = renderGrid();

    // before: row-2, row-1
    // after:  row-1
    // row-2 should stay rendered long enough to fade out.
    currentRows = [currentRows[1]!];
    rerender(renderGridUi());

    expect(getContainingRow(screen.getByText("row-2"))?.getAttribute("data-row-change-state")).toBe(
      "removed",
    );

    act(() => {
      vi.advanceTimersByTime(700);
    });

    expect(screen.queryByText("row-2")).toBeNull();
  });

  it("opens insert sidebar and inserts a new row", () => {
    renderGrid();

    fireEvent.click(screen.getByRole("button", { name: "Insert" }));

    expect(screen.getByRole("heading", { name: "Insert row" })).not.toBeNull();
    expect(screen.getByDisplayValue("auto-generated")).not.toBeNull();

    fireEvent.change(screen.getByLabelText("title"), { target: { value: "new todo" } });
    const doneField = screen.getByLabelText("done field");
    fireEvent.change(within(doneField).getByRole("combobox"), { target: { value: "true" } });
    fireEvent.click(screen.getAllByRole("button", { name: "Insert" })[1] as Element);

    expect(mockInsertDurable).toHaveBeenCalledWith(
      expect.objectContaining({ _table: "todos" }),
      expect.objectContaining({
        title: "new todo",
        done: true,
        meta: null,
      }),
      { tier: "worker" },
    );
  });

  it("closes sidebar when clicking outside", () => {
    renderGrid();

    fireEvent.click(screen.getByRole("button", { name: "Insert" }));
    expect(screen.getByRole("heading", { name: "Insert row" })).not.toBeNull();

    fireEvent.click(screen.getByTestId("row-mutation-overlay"));
    expect(screen.queryByRole("heading", { name: "Insert row" })).toBeNull();
  });

  it("deletes a row when delete is confirmed", async () => {
    renderGrid();

    fireEvent.click(screen.getByRole("gridcell", { name: "zeta" }));

    await waitFor(() => {
      expect(screen.getByDisplayValue("row-2")).not.toBeNull();
    });

    fireEvent.click(screen.getByRole("button", { name: "Delete" }));
    expect(screen.getByText("Delete this row?")).not.toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "Confirm" }));

    await waitFor(() => {
      expect(mockDeleteDurable).toHaveBeenCalledWith(
        expect.objectContaining({ _table: "todos" }),
        "row-2",
        { tier: "worker" },
      );
    });
  });

  it("does not delete when delete is canceled", async () => {
    renderGrid();

    fireEvent.click(screen.getByRole("gridcell", { name: "zeta" }));

    await waitFor(() => {
      expect(screen.getByDisplayValue("row-2")).not.toBeNull();
    });

    fireEvent.click(screen.getByRole("button", { name: "Delete" }));
    fireEvent.click(screen.getByRole("button", { name: "No" }));

    expect(mockDeleteDurable).not.toHaveBeenCalled();
  });
});
